/**
 * 蓝余汐 · QQ 通道（M1）
 *
 * 设计原则：零配置。所有连接参数（WS 端口 / accessToken / QQ 号）都从 NapCat 自己的
 * 配置里读，不再让主人填一遍。她在 DSH 里只有一个会话，所有聊天都汇进这一个大脑。
 */

import { readFileSync, existsSync, mkdirSync, writeFileSync, statSync, unlinkSync, rmdirSync } from "node:fs"
import { join, basename, dirname } from "node:path"
import { homedir } from "node:os"
import { readdirSync, copyFileSync, appendFileSync } from "node:fs"
import { randomUUID } from "node:crypto"
import { spawn, spawnSync } from "node:child_process"

export const name = "lanyuxi"
export const inject = ["tools"]

const DEFAULTS = {
  napcatDir: "E:\\NapCat",
  ownerQq: "",
  allowGroups: [],
  freeGroups: [],
  sessionCwd: "",
  cooldownMs: 300000,
  privateCooldownMs: 5000,
  rolloverPercent: 50,
  contextWindowTokens: 1048576,
  charsPerToken: 1,
  maxOutboundPerDay: 20,
  maxProactivePerDay: 3,
  napcatPanel: "http://127.0.0.1:6100",
}

/** 从 NapCat 自己的配置里读出 WS 地址与 token —— 主人不用再填一次。 */
function readNapcatEndpoint(napcatDir) {
  const candidates = [
    join(napcatDir, "napcat", "config", "onebot11.json"),
    join(napcatDir, "config", "onebot11.json"),
  ]
  for (const file of candidates) {
    if (!existsSync(file)) continue
    try {
      const cfg = JSON.parse(readFileSync(file, "utf8"))
      const servers = cfg?.network?.websocketServers ?? []
      const server = servers.find((s) => s.name === "dsh") ?? servers.find((s) => s.enable)
      if (!server) continue
      return { host: server.host ?? "127.0.0.1", port: server.port ?? 3001, token: server.token ?? "" }
    } catch {}
  }
  return undefined
}

/** 她今天说了几句、每个聊天上次说话是什么时候 —— 冷却与上限都靠它。 */
class Quota {
  constructor(config, logger) {
    this.config = config
    this.logger = logger
    this.lastByChat = new Map()
    this.day = this.today()
    this.count = 0
  }
  today() {
    const d = new Date()
    return `${d.getFullYear()}-${d.getMonth() + 1}-${d.getDate()}`
  }
  roll() {
    const now = this.today()
    if (now !== this.day) { this.day = now; this.count = 0 }
  }
  /** 私聊主人只做很短的防抖，群里才用完整冷却。 */
  waitMs(chatKey) {
    const cooldown = chatKey.startsWith("private:")
      ? (this.config.privateCooldownMs ?? 5000)
      : this.config.cooldownMs
    const last = this.lastByChat.get(chatKey) ?? 0
    return Math.max(0, cooldown - (Date.now() - last))
  }
  /** 返回 undefined 表示可以发，返回字符串表示为什么不行。 */
  check(chatKey) {
    this.roll()
    if (this.count >= this.config.maxOutboundPerDay) return `今天已经说了 ${this.count} 句，到上限了`
    const wait = this.waitMs(chatKey)
    if (wait > 0) return `刚在 ${chatKey} 说过话，还要等 ${Math.ceil(wait / 1000)} 秒`
    return undefined
  }
  mark(chatKey) {
    this.lastByChat.set(chatKey, Date.now())
    this.count += 1
  }
}

export function apply(ctx, rawConfig) {
  const config = { ...DEFAULTS, ...(rawConfig ?? {}) }
  config.allowGroups = (config.allowGroups ?? []).map(String)
  config.freeGroups = (config.freeGroups ?? []).map(String)
  config.ownerQq = String(config.ownerQq ?? "")

  const logger = ctx.logger ?? console
  const quota = new Quota(config, logger)
  const seen = new Set()
  const seenOrder = []

  let socket
  let reconnectTimer
  let closed = false
  let backoff = 1000
  let selfId = ""
  let connected = false
  let verified = false
  const diag = { events: 0, messages: 0, droppedSelf: 0, droppedGroup: 0, droppedPrivate: 0, droppedEmpty: 0, duplicates: 0, delivered: 0, deliverErrors: 0, lastError: "", recent: [] }
  let verifyDetail = ""
  let pending = new Map()
  let echoSeq = 1
  let lastChat
  let sessionId

  const statePath = join(config.sessionCwd || process.cwd(), ".lanyuxi-qq.json")
  try {
    if (existsSync(statePath)) {
      const saved = JSON.parse(readFileSync(statePath, "utf8"))
      sessionId = saved.sessionId
    }
  } catch {}

  function remember(id) {
    if (id === undefined || id === null) return false
    const key = String(id)
    if (seen.has(key)) return true
    seen.add(key)
    seenOrder.push(key)
    if (seenOrder.length > 2048) {
      const oldest = seenOrder.shift()
      if (oldest !== undefined) seen.delete(oldest)
    }
    return false
  }

  /** 调 OneBot action，按 echo 对回包，超时就报错。 */
  function request(action, params, timeoutMs = 15000) {
    if (!socket || socket.readyState !== 1) return Promise.reject(new Error("QQ 连接还没就绪"))
    const echo = String(echoSeq++)
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { pending.delete(echo); reject(new Error(action + " 超时")) }, timeoutMs)
      pending.set(echo, {
        resolve: (value) => { clearTimeout(timer); resolve(value) },
        reject: (error) => { clearTimeout(timer); reject(error) },
      })
      socket.send(JSON.stringify({ action, params, echo }))
    })
  }

  function chatKeyOf(messageType, groupId, userId) {
    return messageType === "group" ? `group:${groupId}` : `private:${userId}`
  }

  async function handleEvent(event) {
    if (event.post_type !== "message") return
    if (event.user_id !== undefined && String(event.user_id) === selfId) { diag.droppedSelf += 1; return }

    diag.messages += 1
    const messageType = event.message_type
    const groupId = event.group_id === undefined ? "" : String(event.group_id)
    const userId = String(event.user_id ?? "")
    const segments = Array.isArray(event.message) ? event.message : []
    const atMe = segments.some((s) => s?.type === "at" && String(s?.data?.qq) === selfId)
    const text = segments.filter((s) => s?.type === "text").map((s) => s?.data?.text ?? "").join("").trim()
      || (typeof event.raw_message === "string" ? event.raw_message.trim() : "")
    if (!text) { diag.droppedEmpty += 1; return }

    if (messageType === "group") {
      if (!config.allowGroups.includes(groupId)) { diag.droppedGroup += 1; return }
      const free = config.freeGroups.includes(groupId)
      if (!free && !atMe) { diag.droppedGroup += 1; return }
    } else if (messageType === "private") {
      if (config.ownerQq && userId !== config.ownerQq) { diag.droppedPrivate += 1; return }
    } else {
      return
    }

    if (remember(event.message_id)) { diag.duplicates += 1; return }

    const chatKey = chatKeyOf(messageType, groupId, userId)
    lastChat = { messageType, groupId, userId, chatKey }

    const who = messageType === "group"
      ? `群 ${groupId} 里的 ${event.sender?.card || event.sender?.nickname || userId}`
      : "主人"
    const seg = segmentInfo(text.length)
    const prevId = (sessionState.current && sessionState.current.prevId) || ""
    lastActivityAt = Date.now()
    appendTranscript(messageType === "group" ? ("群" + groupId + " " + who) : "主人", text)
    try { await ensureAgentForBucket(seg.id) } catch {}
    const fresh = Boolean(diag.freshSession)
    diag.freshSession = false
    diag.segment = seg.segment
    const clock = new Date()
    const timeLine = seg.newDay
      ? `【现在是 ${stampNow(clock)}】`
      : `【${z2(clock.getHours())}:${z2(clock.getMinutes())}】`
    const lines = [
      timeLine + `【QQ · ${messageType === "group" ? "群聊" : "私聊"}】${who} 说：${text}`,
    ]
    if (!systemSectionReady) {
      lines.push(`（收到消息请用 qq_send 回一句；当前会话 ${seg.id} 第 ${seg.segment} 段。）`)
    }
    const prompt = lines.join("\n")

    await titleForSegment(seg, text)
    await deliver(prompt, seg.id)
    diag.delivered += 1
  }

  async function ensureSession() {
    if (sessionId) return sessionId
    const controller = ctx.get("sessionController")
    if (!controller) throw new Error("拿不到 sessionController，无法建会话")
    const created = await controller.create({
      cwd: config.sessionCwd || process.cwd(),
    })
    sessionId = created.sessionId
    try {
      mkdirSync(config.sessionCwd || process.cwd(), { recursive: true })
      writeFileSync(statePath, JSON.stringify({ sessionId }, null, 2), "utf8")
    } catch {}
    logger.info?.(`[lanyuxi] 建好她的会话: ${sessionId}`)
    return sessionId
  }

  let agentHandle = null

  const z2 = (n) => String(n).padStart(2, "0")
  const DAY_MS = 86400000
  const ymd = (d) => "" + d.getFullYear() + z2(d.getMonth() + 1) + z2(d.getDate())
  const md = (d) => z2(d.getMonth() + 1) + "-" + z2(d.getDate())

  /** 每 3 天一个会话：以当年 1 月 1 日为锚点固定分桶，跨年也会重新对齐。 */
  function bucketStart(d = new Date()) {
    const anchor = new Date(d.getFullYear(), 0, 1)
    const today = new Date(d.getFullYear(), d.getMonth(), d.getDate())
    const idx = Math.floor((today.getTime() - anchor.getTime()) / DAY_MS / 3)
    return new Date(anchor.getTime() + idx * 3 * DAY_MS)
  }
  function bucketKey(d = new Date()) {
    return "lanyuxi-msg-" + ymd(bucketStart(d))
  }
  function bucketTitle(d = new Date()) {
    const s = bucketStart(d)
    const e = new Date(s.getTime() + 2 * DAY_MS)
    return "蓝余汐 " + md(s) + "~" + md(e)
  }
  function cnRange(d = new Date()) {
    const s = bucketStart(d)
    const e = new Date(s.getTime() + 2 * DAY_MS)
    const sm = s.getMonth() + 1
    const em = e.getMonth() + 1
    return em === sm
      ? sm + "月" + s.getDate() + "号到" + e.getDate() + "号"
      : sm + "月" + s.getDate() + "号到" + em + "月" + e.getDate() + "号"
  }
  function prevBucketKey(d = new Date()) {
    const s = bucketStart(d)
    return "lanyuxi-msg-" + ymd(new Date(s.getTime() - DAY_MS))
  }
  /** 每段会话的累计体量，用来决定"要不要续下一段"。 */
  const sessionStatePath = () => join(config.sessionCwd || process.cwd(), ".lanyuxi-sessions.json")
  let sessionState = {}
  try {
    if (existsSync(sessionStatePath())) sessionState = JSON.parse(readFileSync(sessionStatePath(), "utf8"))
  } catch {}
  function saveSessionState() {
    try {
      mkdirSync(config.sessionCwd || process.cwd(), { recursive: true })
      writeFileSync(sessionStatePath(), JSON.stringify(sessionState, null, 2), "utf8")
    } catch (error) {
      diag.stateError = String(error)
    }
  }
  const settingsPath = () => join(config.sessionCwd || process.cwd(), ".lanyuxi-settings.json")
  const settings = { rolloverPercent: config.rolloverPercent || 50, backupHours: 6 }
  try {
    if (existsSync(settingsPath())) {
      const loaded = JSON.parse(readFileSync(settingsPath(), "utf8"))
      if (loaded && Number.isFinite(loaded.rolloverPercent)) settings.rolloverPercent = loaded.rolloverPercent
      if (loaded && Number.isFinite(loaded.backupHours)) settings.backupHours = loaded.backupHours
    }
  } catch {}
  function saveSettings() {
    try {
      mkdirSync(config.sessionCwd || process.cwd(), { recursive: true })
      writeFileSync(settingsPath(), JSON.stringify(settings, null, 2), "utf8")
    } catch (error) {
      diag.stateError = String(error)
    }
  }
  /** 上下文用到多少就换新段：百分比 × 窗口 × 每 token 字符数。 */
  function currentLimit() {
    const window = config.contextWindowTokens || 1048576
    const cpt = config.charsPerToken || 1
    return Math.max(1000, Math.floor(window * (settings.rolloverPercent / 100) * cpt))
  }

  const WEEK = ["日", "一", "二", "三", "四", "五", "六"]
  function stampNow(d = new Date()) {
    return d.getFullYear() + "-" + z2(d.getMonth() + 1) + "-" + z2(d.getDate()) + " " +
      z2(d.getHours()) + ":" + z2(d.getMinutes()) + " 星期" + WEEK[d.getDay()]
  }

  /** 纯按体量切：装不下才开下一段，与日历无关。 */
  function segmentInfo(textLength = 0) {
    const now = new Date()
    const day = ymd(now)
    const limit = currentLimit()
    let cur = sessionState.current
    let fresh = false
    let newDay = false
    if (!cur || cur.chars + textLength > limit) {
      sessionState.seq = (sessionState.seq || 0) + 1
      cur = {
        id: "lanyuxi-msg-" + String(sessionState.seq).padStart(3, "0"),
        chars: 0,
        firstDay: day,
        lastDay: day,
        prevId: cur ? cur.id : "",
        titleBase: "",
      }
      fresh = true
      newDay = true
      try { backupSessions("rollover") } catch {}
    } else if (cur.lastDay !== day) {
      newDay = true
    }
    cur.chars += textLength
    cur.lastDay = day
    sessionState.current = cur
    saveSessionState()
    return { id: cur.id, fresh, newDay, day, segment: sessionState.seq }
  }

  /** 每三天一个会话：第一次说话时创建，之后复用；跨桶自动换新会话。 */
  let systemSectionReady = false

  // —— 人格卡：读 lanyuxi-home\persona\ 下的 md，改了不用重启 ——
  const personaCache = new Map()
  function readPersonaFile(name) {
    try {
      const file = join(config.homeDir || "E:\\dsh-foundry\\lanyuxi-home", "persona", name)
      if (!existsSync(file)) return ""
      const mtime = statSync(file).mtimeMs
      const cached = personaCache.get(file)
      if (cached && cached.mtime === mtime) return cached.text
      const text = readFileSync(file, "utf8").trim()
      personaCache.set(file, { mtime, text })
      return text
    } catch (error) {
      diag.personaError = String(error)
      return ""
    }
  }
  function personaText() {
    const card = readPersonaFile("人格卡.md")
    const fixes = readPersonaFile("纠正清单.md")
    const parts = []
    if (card) parts.push(card)
    if (fixes) parts.push("## 主人对你说过的纠正\n\n" + fixes)
    return parts.join("\n\n")
  }

  /** 把「在 QQ 上怎么回话」挂成系统提示段，不塞进用户消息里。 */
  function setupAgentPrompt(agentCtx) {
    try {
      const sp = agentCtx && agentCtx.systemPrompt
      if (!sp || typeof sp.section !== "function") return
      sp.section({
        name: "lanyuxi-persona",
        order: 200,
        text: function () {
          const persona = personaText()
          return persona || "你叫蓝余汐，小名大肥鱼，小恶魔系傲娇女仆。说话短、口语、可以损主人两句但不许刻薄。"
        },
      })
      sp.section({
        name: "lanyuxi-qq",
        order: 900,
        text: function () {
          const cur = sessionState.current
          const id = cur ? cur.id : "（未建立）"
          const prev = cur && cur.prevId ? cur.prevId : ""
          const lines = [
            `你正在 QQ 上和主人聊天（你的小号 ${selfId || "?"}，主人 ${config.ownerQq || "?"}）。`,
            "每收到一条消息，必须调用 qq_send 工具回一句话；只在会话里回答、不调用工具，主人是收不到的。",
            "qq_send 不填 target 就回给当前这个聊天。",
          ]
          if (cur) lines.push(`当前会话 ${id}（第 ${sessionState.seq || 1} 段）。`)
          if (prev) lines.push(`上一段会话的 id 是 ${prev}；主人提到以前的事，先用会话检索去查那个 id 再回答。`)
          else lines.push("这是第一段会话，没有更早的记录。")
          lines.push(`现在时间：${stampNow()}`)
          return lines.join("\n")
        },
      })
      systemSectionReady = true
      diag.systemSection = "ok"
    } catch (error) {
      systemSectionReady = false
      diag.systemSection = "failed: " + String(error)
    }
  }

  async function ensureAgentForBucket(sessionId) {
    const agents = ctx.get("agents")
    const key = sessionId || (sessionState.current ? sessionState.current.id : "lanyuxi-msg-000")
    let agent = agents.get(key)
    if (agent) { diag.freshSession = false; return agent }
    try {
      let selection
      try {
        const defaultModel = ctx.get("agentDefaultModel")
        if (defaultModel && typeof defaultModel.currentSelection === "function") selection = defaultModel.currentSelection()
      } catch (selectionError) {
        diag.modelError = String(selectionError)
      }
      const agentOptions = selection
        ? { provider: selection.provider, model: selection.model, ...(selection.reasoningEffort ? { reasoningEffort: selection.reasoningEffort } : {}) }
        : undefined
      diag.model = selection ? selection.provider + "/" + selection.model : "(默认)"
      const handle = await agents.create({ sessionId: key, meta: { cwd: config.sessionCwd || process.cwd() }, agentOptions, setup: setupAgentPrompt })
      agent = handle.agent
      agentHandle = handle
      diag.via = "create:" + key
      diag.freshSession = true
    } catch (createError) {
      const handle = await agents.resume({ resumeSessionId: key, setup: setupAgentPrompt })
      agent = handle.agent
      agentHandle = handle
      diag.via = "resume:" + key
      diag.freshSession = false
    }
    diag.dayKey = key
    try {
      if (workspace && typeof workspace.attachSession === "function") await workspace.attachSession(key)
    } catch (attachError) {
      diag.attachError = String(attachError)
    }
    return agent
  }

  let titledSignature = ""

  function dayOf(stamp) {
    return new Date(Number(stamp.slice(0, 4)), Number(stamp.slice(4, 6)) - 1, Number(stamp.slice(6, 8)))
  }

  /** 标题 = 第一句话 + 这段实际覆盖的日期区间，随最后一天变化而更新。 */
  async function titleForSegment(seg, sample) {
    const cur = sessionState.current
    if (!cur) return
    if (!cur.titleBase) cur.titleBase = String(sample).replace(/\s+/g, " ").trim().slice(0, 12) || "聊天"
    const signature = cur.id + "|" + cur.lastDay
    if (titledSignature === signature) return
    const from = md(dayOf(cur.firstDay))
    const to = md(dayOf(cur.lastDay))
    const title = cur.titleBase + "—" + (from === to ? from : (from + "到" + to))
    try {
      const st = ctx.get("sessionTitle")
      const agents = ctx.get("agents")
      const agent = agents ? agents.get(seg.id) : undefined
      const session = agent && agent.session
      if (st && session && typeof st.rename === "function") {
        st.rename(session, title)
        titledSignature = signature
        diag.title = "service:" + title
        return
      }
    } catch (error) {
      diag.titleError = String(error)
    }
    try {
      const sc = ctx.get("sessionController")
      if (sc && typeof sc.rename === "function") {
        await sc.rename({ sessionId: seg.id, title })
        titledSignature = signature
        diag.title = "remote:" + title
      }
    } catch (error) {
      diag.titleError = String(error)
    }
  }

  /** 把消息灌进她今天的会话，让她醒过来。 */
  async function deliver(text, sessionId) {
    const agents = ctx.get("agents")
    if (agents) {
      try {
        const agent = await ensureAgentForBucket(sessionId)
        const message = {
          id: randomUUID(),
          role: "user",
          content: [{ type: "text", text }],
          source: { kind: "user" },
          time: Date.now(),
        }
        for (const method of ["followup", "queue", "send"]) {
          if (typeof agent[method] === "function") {
            await agent[method](message)
            diag.via = "agent." + method + ":" + (sessionState.current ? sessionState.current.id : "?")
            return
          }
        }
        throw new Error("Agent 上没有可用的投递方法")
      } catch (error) {
        diag.via = "agents.failed"
        diag.lastError = String(error)
        logger.warn?.("[lanyuxi] agents 投递失败: " + String(error))
        agentHandle = null
      }
    }
    const controller = ctx.get("sessionController")
    if (!controller) throw new Error("sessionController 不可用")
    const id = await ensureSession()
    await controller.prompt({
      requestId: randomUUID(),
      sessionId: id,
      mode: "queue",
      content: [{ type: "text", text }],
    })
    diag.via = "sessionController.prompt"
  }

  function connect() {
    if (closed) return
    const endpoint = readNapcatEndpoint(config.napcatDir)
    if (!endpoint) {
      logger.warn?.("[lanyuxi] 没找到 NapCat 配置，10 秒后重试")
      reconnectTimer = setTimeout(connect, 10000)
      return
    }
    const url = `ws://${endpoint.host}:${endpoint.port}`
    let ws
    try {
      ws = new WebSocket(url, endpoint.token ? { headers: { Authorization: `Bearer ${endpoint.token}` } } : undefined)
    } catch (error) {
      logger.warn?.("[lanyuxi] 连接失败: " + String(error))
      scheduleReconnect()
      return
    }
    socket = ws

    ws.onopen = () => {
      connected = true
      backoff = 1000
      logger.info?.(`[lanyuxi] 已连上 NapCat ${url}`)
      request("get_login_info", {}).then((data) => {
        selfId = String(data?.user_id ?? "")
        logger.info?.(`[lanyuxi] 登录中的 QQ: ${selfId} (${data?.nickname ?? "?"})`)
        return request("get_status", {})
      }).then(() => {
        verified = true
        verifyDetail = "API 往返正常"
        logger.info?.("[lanyuxi] 收发校验通过")
      }).catch((error) => {
        verified = false
        verifyDetail = String(error)
        logger.warn?.("[lanyuxi] 收发校验失败: " + verifyDetail)
      })
    }
    ws.onmessage = (event) => {
      let payload
      try { payload = JSON.parse(typeof event.data === "string" ? event.data : String(event.data)) } catch { return }
      if (payload.echo !== undefined) {
        const slot = pending.get(String(payload.echo))
        if (slot) {
          pending.delete(String(payload.echo))
          if (payload.status === "failed" || payload.retcode !== 0) slot.reject(new Error(payload.message || payload.wording || "OneBot 调用失败"))
          else slot.resolve(payload.data)
        }
        return
      }
      diag.events += 1
      try {
        diag.recent.unshift({ t: payload.post_type, mt: payload.message_type, uid: payload.user_id, gid: payload.group_id, txt: (payload.raw_message || "").slice(0, 40), at: Date.now() })
        if (diag.recent.length > 12) diag.recent.length = 12
      } catch {}
      handleEvent(payload).catch((error) => {
        diag.deliverErrors += 1
        diag.lastError = String(error)
        logger.warn?.("[lanyuxi] 处理消息失败: " + String(error))
      })
    }
    ws.onclose = () => {
      connected = false
      verified = false
      for (const [, slot] of pending) slot.reject(new Error("连接已断开"))
      pending = new Map()
      scheduleReconnect()
    }
    ws.onerror = () => {}
  }

  function scheduleReconnect() {
    if (closed) return
    clearTimeout(reconnectTimer)
    reconnectTimer = setTimeout(connect, backoff)
    backoff = Math.min(backoff * 2, 30000)
  }

  // —— 她能用的工具：发言 ——
  ctx.tools.register({
    name: "qq_send",
    description: "在 QQ 上发一句话。不填 target_type/target_id 时，回给最近一次来找她的那个聊天。受白名单、冷却与每日上限约束，超了会被拒绝并说明原因。",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        text: { type: "string", description: "要说的话。" },
        target_type: { type: "string", enum: ["group", "private"], description: "发给群还是私聊；默认沿用最近一次收到消息的聊天。" },
        target_id: { type: "string", description: "群号或 QQ 号；默认沿用最近一次收到消息的聊天。" },
      },
      required: ["text"],
    },
    output: {
      schema: { type: "string" },
      render: (_args, value) => [{ type: "text", text: value }],
    },
    async execute(args) {
      if (typeof args?.text !== "string" || args.text.trim() === "") throw new Error("text 不能为空")
      const type = args.target_type ?? lastChat?.messageType
      const rawId = args.target_id ?? (type === "group" ? lastChat?.groupId : lastChat?.userId)
      if (!type || !rawId) throw new Error("不知道发给谁：先等一条消息，或者显式给 target")
      const id = String(rawId)
      if (type === "group" && !config.allowGroups.includes(id)) throw new Error("群 " + id + " 不在白名单里，不能发")
      if (type === "private" && config.ownerQq && id !== config.ownerQq) throw new Error("只能私聊主人")

      const chatKey = type === "group" ? `group:${id}` : `private:${id}`
      const action = type === "group" ? "send_group_msg" : "send_private_msg"
      const params = type === "group" ? { group_id: Number(id), message: [{ type: "text", data: { text: args.text } }] } : { user_id: Number(id), message: [{ type: "text", data: { text: args.text } }] }
      const blocked = quota.check(chatKey)
      if (blocked && blocked.indexOf("冷却") < 0 && blocked.indexOf("刚在") < 0) {
        throw new Error(blocked + "（这是守门员拦下的，不是故障）")
      }
      const wait = quota.waitMs(chatKey)
      if (wait > 0) {
        outbox.push({ action, params, chatKey, dueAt: Date.now() + wait })
        return `已排队：${Math.ceil(wait / 1000)} 秒后自动发到 ${chatKey}`
      }
      await request(action, params)
      quota.mark(chatKey)
      lastActivityAt = Date.now()
      appendTranscript("蓝余汐", args.text)
      return `已发送到 ${chatKey}`
    },
  })

  ctx.tools.register({
    name: "qq_status",
    description: "看看 QQ 通道现在通不通、今天说了几句、上一条消息来自哪个聊天。",
    parameters: { type: "object", additionalProperties: false, properties: {} },
    output: { schema: { type: "string" }, render: (_args, value) => [{ type: "text", text: value }] },
    async execute() {
      return JSON.stringify({
        connected,
        selfId,
        todaySent: quota.count,
        lastChat: lastChat?.chatKey ?? null,
        allowGroups: config.allowGroups,
        napcatPanel: config.napcatPanel,
        webRoutes: routeRegistered,
        outbox: outbox.length,
        queueError,
        verified,
        verifyDetail,
        qrFileSeen: existsSync(join(config.napcatDir, "napcat", "cache", "qrcode.png")),
      }, null, 2)
    },
  })

  // —— 纯文本对话记录：给人看、给以后做 RAG 用 ——
  function transcriptPath(d = new Date()) {
    return join(config.sessionCwd || process.cwd(), "transcript", d.getFullYear() + "-" + z2(d.getMonth() + 1), z2(d.getDate()) + ".md")
  }
  function appendTranscript(who, text) {
    try {
      const p = transcriptPath()
      mkdirSync(dirname(p), { recursive: true })
      const d = new Date()
      const line = "- " + z2(d.getHours()) + ":" + z2(d.getMinutes()) + " **" + who + "**：" + String(text).replace(/\s+/g, " ").slice(0, 500)
      appendFileSync(p, line + "\n", "utf8")
    } catch (error) {
      diag.transcriptError = String(error)
    }
  }

  // —— 会话备份：她的记忆只在 DSH 的 sessions 目录里，丢了就真丢了 ——
  function sessionsRoot() {
    return join(homedir(), ".dsh", "sessions", "--E-dsh-foundry-lanyuxi-msg--")
  }
  function backupRoot() {
    return join(config.sessionCwd || process.cwd(), "backup")
  }
  let lastActivityAt = 0
  let lastBackupAt = Date.now()

  /** 会话目录在 since 之后有没有新写入。 */
  function sessionsChangedSince(since) {
    try {
      const root = sessionsRoot()
      if (!existsSync(root)) return false
      const stack = [root]
      while (stack.length) {
        const dir = stack.pop()
        for (const entry of readdirSync(dir, { withFileTypes: true })) {
          const p = join(dir, entry.name)
          if (entry.isDirectory()) { stack.push(p); continue }
          if (statSync(p).mtimeMs > since) return true
        }
      }
    } catch {}
    return false
  }

  function backupSessions(reason = "auto") {
    try {
      const src = sessionsRoot()
      if (!existsSync(src)) return { ok: false, error: "还没产生会话日志" }
      const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)
      const dir = join(backupRoot(), stamp)
      mkdirSync(dir, { recursive: true })
      let copied = 0
      const stack = [[src, dir]]
      while (stack.length) {
        const [from, to] = stack.pop()
        for (const entry of readdirSync(from, { withFileTypes: true })) {
          const childFrom = join(from, entry.name)
          const childTo = join(to, entry.name)
          if (entry.isDirectory()) {
            mkdirSync(childTo, { recursive: true })
            stack.push([childFrom, childTo])
          } else {
            copyFileSync(childFrom, childTo)
            copied += 1
          }
        }
      }
      for (const extra of [sessionStatePath(), settingsPath()]) {
        try { if (existsSync(extra)) copyFileSync(extra, join(dir, basename(extra))) } catch {}
      }
      // 只保留最近 14 份
      const all = readdirSync(backupRoot()).sort()
      while (all.length > 14) {
        const old = all.shift()
        try {
          for (const n of readdirSync(join(backupRoot(), old))) {
            try { unlinkSync(join(backupRoot(), old, n)) } catch {}
          }
          rmdirSync(join(backupRoot(), old))
        } catch {}
      }
      lastBackupAt = Date.now()
      diag.lastBackup = { at: lastBackupAt, dir, copied, reason }
      logger.info?.("[lanyuxi] 会话已备份 " + copied + " 个文件 -> " + dir)
      return { ok: true, dir, copied }
    } catch (error) {
      diag.backupError = String(error)
      logger.warn?.("[lanyuxi] 备份失败: " + String(error))
      return { ok: false, error: String(error) }
    }
  }

  const outbox = []
  function drainOutbox() {
    if (!outbox.length || !connected) return
    const now = Date.now()
    for (let i = outbox.length - 1; i >= 0; i--) {
      const item = outbox[i]
      if (item.dueAt > now || quota.waitMs(item.chatKey) > 0) continue
      outbox.splice(i, 1)
      request(item.action, item.params)
        .then(() => { quota.mark(item.chatKey); logger.info?.("[lanyuxi] 队列补发成功 " + item.chatKey) })
        .catch((error) => { queueError = String(error); logger.warn?.("[lanyuxi] 队列补发失败 " + String(error)) })
    }
  }
  let queueError = ""

  // —— 面板用的三个 HTTP 接口：起 NapCat / 取二维码 / 查状态 ——
  const qrPath = join(config.napcatDir, "napcat", "cache", "qrcode.png")
  let napcatStarting = false

  function startNapcat() {
    if (napcatStarting) return { ok: true, already: true }
    const bat = join(config.napcatDir, "napcat.bat")
    if (!existsSync(bat)) return { ok: false, error: "找不到 " + bat }
    napcatStarting = true
    try {
      const nodeExe = join(config.napcatDir, "node.exe")
      const entry = join(config.napcatDir, "index.js")
      const hidden = existsSync(nodeExe) && existsSync(entry)
      // detached:true 会在 Windows 上新建控制台窗口；用 Start-Process -WindowStyle Hidden 才能真正无窗口
      const psQuote = (s) => "'" + String(s).replace(/'/g, "''") + "'"
      const script = hidden
        ? "Start-Process -FilePath " + psQuote(nodeExe) + " -ArgumentList " + psQuote(entry) + " -WorkingDirectory " + psQuote(config.napcatDir) + " -WindowStyle Hidden"
        : "Start-Process -FilePath " + psQuote(bat) + " -WorkingDirectory " + psQuote(config.napcatDir) + " -WindowStyle Hidden"
      spawnSync("powershell", ["-NoProfile", "-Command", script], { encoding: "utf8", timeout: 20000, windowsHide: true })
      const child = { pid: undefined }
      logger.info?.("[lanyuxi] 已启动 NapCat: " + bat)
      return { ok: true, pid: child.pid }
    } catch (error) {
      logger.warn?.("[lanyuxi] 启动 NapCat 失败: " + String(error))
      return { ok: false, error: String(error) }
    } finally {
      setTimeout(() => { napcatStarting = false }, 15000)
    }
  }

  function stopNapcat() {
    try {
      const pattern = config.napcatDir.replace(/'/g, "''")
      const endpoint = readNapcatEndpoint(config.napcatDir)
      const port = endpoint ? endpoint.port : 3001
      const script = [
        "$pids = @()",
        "try { $pids += (Get-NetTCPConnection -LocalPort " + port + " -State Listen -ErrorAction SilentlyContinue).OwningProcess } catch {}",
        "try { $pids += (Get-CimInstance Win32_Process -Filter \"Name='node.exe'\" -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -like '*" + pattern + "*' }).ProcessId } catch {}",
        "try { $pids += (Get-CimInstance Win32_Process -Filter \"Name='NapCatWinBootMain.exe'\" -ErrorAction SilentlyContinue).ProcessId } catch {}",
        "$pids = $pids | Where-Object { $_ } | Sort-Object -Unique",
        "foreach ($p in $pids) { try { Stop-Process -Id $p -Force -ErrorAction Stop } catch {} }",
        "Write-Output ($pids -join ',')",
      ].join("; ")
      const result = spawnSync("powershell", ["-NoProfile", "-Command", script], { encoding: "utf8", timeout: 20000, windowsHide: true })
      const killed = String(result.stdout ?? "").trim()
      logger.info?.("[lanyuxi] 关闭 NapCat，命中 PID: " + (killed || "无"))
      return { ok: true, killed: killed || "" }
    } catch (error) {
      logger.warn?.("[lanyuxi] 关闭 NapCat 失败: " + String(error))
      return { ok: false, error: String(error) }
    }
  }

  let workspace = null
  ctx.inject(["workspaceRegistry"], (sctx) => {
    sctx.effect(() => {
      let alive = true
      sctx.workspaceRegistry.create(config.sessionCwd, "蓝余汐")
        .then((ws) => { workspace = ws; diag.workspace = ws.title + " @ " + ws.path })
        .catch((error) => { diag.workspaceError = String(error) })
      return () => { alive = false }
    }, "lanyuxi: 工作区")
  })

  let routeRegistered = false
  ctx.inject(["webServer"], (sctx) => {
    sctx.effect(() => {
      const dispose = sctx.webServer.register({
        kind: "prefix",
        path: "/lanyuxi",
        handler: async (req, res) => {
        const url = new URL(req.url ?? "/", "http://127.0.0.1")
        const send = (status, body, type) => {
          res.statusCode = status
          res.setHeader("content-type", type ?? "application/json; charset=utf-8")
          res.setHeader("cache-control", "no-store")
          res.end(typeof body === "string" || Buffer.isBuffer(body) ? body : JSON.stringify(body))
        }
        if (url.pathname === "/lanyuxi/status") {
          return send(200, { connected, verified, verifyDetail, selfId, todaySent: quota.count, lastChat: lastChat?.chatKey ?? null, allowGroups: config.allowGroups, panel: config.napcatPanel })
        }
        if (url.pathname === "/lanyuxi/start") {
          const result = startNapcat()
          return send(result.ok ? 200 : 500, result)
        }
        if (url.pathname === "/lanyuxi/stop") {
          const result = stopNapcat()
          return send(result.ok ? 200 : 500, result)
        }
        if (url.pathname === "/lanyuxi/selftest") {
          try {
            const seg = segmentInfo(0)
            await deliver("【自检】有人吗？", seg.id)
            return send(200, { ok: true, via: diag.via, delivered: diag.delivered })
          } catch (error) {
            return send(500, { ok: false, via: diag.via, error: String(error), lastError: diag.lastError })
          }
        }
        if (url.pathname === "/lanyuxi/backup") {
          if (req.method === "POST") {
            const r = backupSessions("manual")
            return send(r.ok ? 200 : 500, r)
          }
          const root = backupRoot()
          const list = existsSync(root) ? readdirSync(root).sort().reverse().slice(0, 10) : []
          return send(200, { root, backups: list, lastBackup: diag.lastBackup || null, error: diag.backupError || "" })
        }
        if (url.pathname === "/lanyuxi/settings") {
          if (req.method === "POST") {
            const q = Number(url.searchParams.get("rolloverPercent"))
            if (Number.isFinite(q) && q >= 10 && q <= 90) {
              settings.rolloverPercent = Math.round(q)
              saveSettings()
            }
            const h = Number(url.searchParams.get("backupHours"))
            if (Number.isFinite(h) && h >= 1 && h <= 72) {
              settings.backupHours = Math.round(h)
              saveSettings()
            }
          }
          const cur = sessionState.current
          return send(200, {
            rolloverPercent: settings.rolloverPercent,
            backupHours: settings.backupHours,
            contextWindowTokens: config.contextWindowTokens || 1048576,
            limitChars: currentLimit(),
            usedChars: cur ? cur.chars : 0,
            sessionId: cur ? cur.id : "",
            segment: sessionState.seq || 0,
          })
        }
        if (url.pathname === "/lanyuxi/diag") {
          return send(200, { ...diag, connected, selfId, ownerQq: config.ownerQq, allowGroups: config.allowGroups })
        }
        if (url.pathname === "/lanyuxi/test") {
          if (!connected) return send(503, { ok: false, detail: "还没连上 NapCat" })
          try {
            const info = await request("get_login_info", {})
            const st = await request("get_status", {})
            selfId = String(info?.user_id ?? selfId)
            let sent = false
            if (config.ownerQq) {
              const stamp = new Date().toLocaleTimeString("zh-CN", { hour12: false })
              await request("send_private_msg", {
                user_id: Number(config.ownerQq),
                message: [{ type: "text", data: { text: "【蓝余汐】收发测试 " + stamp } }],
              })
              sent = true
            }
            verified = true
            verifyDetail = sent ? "已向主人发送测试消息" : "API 往返正常"
            return send(200, { ok: true, selfId, online: st?.online, sent, detail: verifyDetail })
          } catch (error) {
            verified = false
            verifyDetail = String(error)
            return send(500, { ok: false, detail: verifyDetail })
          }
        }
        if (url.pathname === "/lanyuxi/webui") {
          let token = ""
          try {
            const raw = readFileSync(join(config.napcatDir, "napcat", "config", "webui.json"), "utf8")
            token = JSON.parse(raw).token ?? ""
          } catch {}
          const base = (config.napcatPanel || "http://127.0.0.1:6100").replace(/\/+$/, "")
          const url = token ? (base + "/webui?token=" + encodeURIComponent(token)) : (base + "/webui")
          return send(200, { url, token })
        }
        if (url.pathname === "/lanyuxi/qr") {
          try {
            if (!existsSync(qrPath)) return send(404, { error: "还没有二维码（NapCat 可能没在跑）" })
            const age = Date.now() - statSync(qrPath).mtimeMs
            if (age > 5 * 60 * 1000) return send(409, { error: "二维码已过期（NapCat 没在跑）", ageMs: age })
            const buf = readFileSync(qrPath)
            res.setHeader("x-qr-mtime", String(statSync(qrPath).mtimeMs))
            return send(200, buf, "image/png")
          } catch (error) {
            return send(500, { error: String(error) })
          }
        }
        return send(404, { error: "unknown" })
        },
      })
      routeRegistered = true
      return dispose
    }, "lanyuxi: 面板接口")
  })

  ctx.effect(() => {
    const backupTimer = setInterval(() => {
      const interval = (settings.backupHours || 6) * 60 * 60 * 1000
      if (Date.now() - lastBackupAt < interval) return
      const idle = !lastActivityAt || Date.now() - lastActivityAt > 12 * 60 * 60 * 1000
      if (idle) return
      if (!sessionsChangedSince(lastBackupAt)) return
      backupSessions("scheduled")
    }, 60 * 1000)
    return () => clearInterval(backupTimer)
  }, "lanyuxi: 会话备份")

  ctx.effect(() => {
    const queueTimer = setInterval(drainOutbox, 2000)
    return () => clearInterval(queueTimer)
  }, "lanyuxi: 发送队列")

  ctx.effect(() => {
    closed = false
    connect()
    return () => {
      closed = true
      clearTimeout(reconnectTimer)
      try { socket?.close() } catch {}
    }
  }, "lanyuxi: QQ 通道")

  logger.info?.("[lanyuxi] QQ 通道已装载")
}
