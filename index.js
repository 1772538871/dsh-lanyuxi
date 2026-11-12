/**
 * 蓝余汐 · QQ 通道（M1）
 *
 * 设计原则：零配置。所有连接参数（WS 端口 / accessToken / QQ 号）都从 NapCat 自己的
 * 配置里读，不再让主人填一遍。她在 DSH 里只有一个会话，所有聊天都汇进这一个大脑。
 */

import { readFileSync, existsSync, mkdirSync, writeFileSync, statSync, unlinkSync, rmdirSync, rmSync } from "node:fs"
import { join, basename, dirname } from "node:path"
import { homedir } from "node:os"
import { readdirSync, copyFileSync, appendFileSync } from "node:fs"
import { randomUUID, createHash } from "node:crypto"
import { spawn, spawnSync } from "node:child_process"
import { logEvent, messageSummary, readGrowth } from "./memory-log.js"

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
  groupOutboundPerDay: 10,
  gptSovitsDir: "E:\\GPT-SoVITS",
  gptSovitsApi: "http://127.0.0.1:9880",
  gptSovitsPort: 9880,
  voiceEnabled: true,
  voiceEngine: "auto",
  voiceRealtime: false,
  pythonPath: "python",
  edgeTtsVoice: "zh-CN-XiaoxiaoNeural",
  sapiVoice: "Microsoft Huihui Desktop",
  rolloverPercent: 50,
  contextWindowTokens: 1048576,
  charsPerToken: 1,
  maxOutboundPerDay: 20,

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
  let selfNickname = ""
  let connected = false
  let verified = false
  const diag = { events: 0, messages: 0, droppedSelf: 0, droppedGroup: 0, droppedPrivate: 0, droppedEmpty: 0, duplicates: 0, delivered: 0, deliverErrors: 0, lastError: "", memoryError: "", recent: [] }
  let verifyDetail = ""
  let pending = new Map()
  let echoSeq = 1
  let lastChat
  /** 还在等她回话的聊天（每次投递进一条，成功回复后清掉）——用来替掉 lastChat 的隐式寻址。 */
  const pendingChats = []
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
    if (messageType === "group") diag.groupSeen = (diag.groupSeen || 0) + 1
    if (atMe) diag.atSeen = (diag.atSeen || 0) + 1
    diag.lastMsg = {
      at: Date.now(),
      type: messageType,
      gid: groupId,
      uid: userId,
      atMe,
      selfId,
      segTypes: segments.map((s) => s?.type).join(","),
      raw: String(event.raw_message || "").slice(0, 80),
    }
    let text = segments.filter((s) => s?.type === "text").map((s) => s?.data?.text ?? "").join("").trim()
    if (!text && typeof event.raw_message === "string") text = event.raw_message.replace(/\[CQ:[^\]]*\]/g, "").trim()
    if (!text) { diag.droppedEmpty += 1; return }

    if (messageType === "group") {
      if (!config.allowGroups.includes(groupId)) { diag.droppedGroup += 1; return }
    } else if (messageType === "private") {
      if (config.ownerQq && userId !== config.ownerQq) { diag.droppedPrivate += 1; return }
    } else {
      return
    }

    // 去重必须放在写群上下文之前：重复推送的事件不能再污染她的上下文
    if (remember(event.message_id)) { diag.duplicates += 1; return }

    if (messageType === "group") {
      const speaker = event.sender?.card || event.sender?.nickname || String(userId)
      if (text) pushGroupMessage(groupId, speaker, text)
      rememberGroupName(groupId)
      const free = config.freeGroups.includes(groupId)
      if (!free && !atMe) { diag.droppedGroup += 1; return }
    }

    // 图片：下载到她的图片库，再把本地路径告诉她，她可以用 read_image 看
    const imageSegs = segments.filter((s) => s?.type === "image")
    if (imageSegs.length) {
      const saved = []
      for (const seg of imageSegs.slice(0, 4)) {
        const url = seg?.data?.url || seg?.data?.file_url || ""
        const fileName = String(seg?.data?.file || "image").slice(0, 40).replace(/[^\w.\-]/g, "_")
        try {
          if (!url) continue
          const res = await fetch(url)
          if (!res.ok) continue
          const buf = Buffer.from(await res.arrayBuffer())
          const now = new Date()
          const month = now.getFullYear() + "-" + z2(now.getMonth() + 1)
          const dir = join(config.homeDir || "E:\\dsh-foundry\\lanyuxi-home", "images", month)
          mkdirSync(dir, { recursive: true })
          const out = join(dir, Date.now() + "-" + fileName.replace(/\.(jpg|jpeg|png|gif|webp)$/i, "") + (String(url).match(/\.(jpg|jpeg|png|gif|webp)/i) || [".jpg"])[0])
          writeFileSync(out, buf)
          saved.push(out)
          const frames = extractGifFrames(out)
          if (frames.length) saved.push("\u0000GIF\u0000" + frames.join("、"))
        } catch (error) {
          diag.imageError = String(error)
        }
      }
      if (saved.length) {
        const plain = saved.filter((x) => x.indexOf("\u0000GIF\u0000") < 0)
        const gifs = saved.filter((x) => x.indexOf("\u0000GIF\u0000") >= 0).map((x) => x.replace("\u0000GIF\u0000", ""))
        const parts = []
        if (plain.length) parts.push("（对方发了 " + plain.length + " 张图，已存到：" + plain.join("、") + "。想看清楚就用 read_image 打开它。）")
        if (gifs.length) {
          const frameList = gifs.join("、").split("、").filter(Boolean)
          parts.push("（其中是动图，按每秒抽帧共 " + frameList.length + " 帧：\n" + frameList.join("\n") + "\n 逐帧 read_image 看，能看出它在动什么、想表达什么，别只看第一帧。）")
        }
        text = (text ? text + "\n" : "") + parts.join("\n")
      } else {
        text = (text ? text + "\n" : "") + "（对方发了一张图，但没下载下来，你只看到一条图片提示。）"
      }
    }
    const chatKey = chatKeyOf(messageType, groupId, userId)
    lastChat = { messageType, groupId, userId, chatKey }

    const who = messageType === "group"
      ? `群 ${groupId} 里的 ${event.sender?.card || event.sender?.nickname || userId}`
      : ownerLabel()
    if (muted()) { diag.mutedSkip = (diag.mutedSkip || 0) + 1; return }
    if (brakes.socialPaused && messageType === "group") { diag.socialSkip = (diag.socialSkip || 0) + 1; return }

    // 真的投递给她了，才登记为「等她回话」
    pendingChats.push({ messageType, groupId, userId, chatKey, at: Date.now() })
    while (pendingChats.length > 20) pendingChats.shift()
    // 记忆 v0：投递成功了才记一条。写日志出错只记账，绝不许把这条消息搞挂
    try {
      logEvent(config.homeDir || "E:\\dsh-foundry\\lanyuxi-home", {
        kind: "message",
        platform: "qq",
        who: messageType === "group" ? "群 " + groupId : "主人",
        perms: messageType === "group" ? "shared" : "private",
        summary: messageSummary(text),
        ref: event.message_id,
      })
    } catch (error) {
      diag.memoryError = String(error)
      logger.warn?.("[lanyuxi] 记忆日志写不进去: " + String(error))
    }
    // —— 每日日记：新的一天第一次说话时，让她把昨天写下来 ——
    const diaryKey = ymd(new Date())
    let diaryTask = ""
    if (sessionState.lastDiaryDay !== diaryKey) {
      const prev = sessionState.lastDiaryDay
      sessionState.lastDiaryDay = diaryKey
      saveSessionState()
      if (prev) {
        const d = new Date()
        const month = d.getFullYear() + "-" + z2(d.getMonth() + 1)
        const dayName = diaryKey
        const base = config.homeDir || "E:\\dsh-foundry\\lanyuxi-home"
        const now2 = new Date()
        const week = "日一二三四五六"[now2.getDay()]
        const head = `${now2.getFullYear()}年${now2.getMonth() + 1}月${now2.getDate()}日 星期${week} 天气${"（写 晴/多云/小雨，后面跟 有点热/凉快/冷，不用冒号）"} 心情${"（随手写一个词，如 一般 / 开心 / 烦）"}`
        diaryTask = [
          `【今天的第一件事】现在先写日记，写两篇，都写到 ${base}\\diary\\ 下：`,
          `1) 表日记：${base}\\diary\\to-owner\\${month}\\${dayName}.md —— 给主人看的小学生作文，200 字以内。`,
          `2) 里日记：${base}\\diary\\private\\${month}\\${dayName}.md —— 只给自己看，可以嘴硬、可以承认在意。`,
          `两篇的第一行就照这个样式写（日期 星期 天气 心情），整行不出现冒号，天气只写冷热凉快，不要写几度：`,
          head,
          `第二行空一行，然后自由写正文（不一定要「今天」开头，想怎么写就怎么写），写昨天跟他说了什么、他哪句话惹你笑了或气到你了。`,
          `写完再回这条消息；日记内容不要贴回 QQ。`,
        ].join("\n")
        // 记忆 v0：真的下发了日记任务，才把这条记一笔
        try {
          logEvent(config.homeDir || "E:\\dsh-foundry\\lanyuxi-home", {
            kind: "diary",
            platform: "local",
            who: "她",
            perms: "system",
            summary: "按新格式写今天的两篇日记",
          })
        } catch (error) {
          diag.memoryError = String(error)
          logger.warn?.("[lanyuxi] 记忆日志写不进去: " + String(error))
        }
      }
    }
    const seg = segmentInfo(text.length)
    const prevId = (sessionState.current && sessionState.current.prevId) || ""
    lastActivityAt = Date.now()
    if (messageType === "private" && userId === config.ownerQq) noteOwnerReplied()

    appendTranscript(messageType === "group" ? ("群" + groupId + " " + who) : ownerLabel(), text)
    try { await ensureAgentForBucket(seg.id) } catch {}
    const fresh = Boolean(diag.freshSession)
    diag.freshSession = false
    diag.segment = seg.segment
    const clock = new Date()
    const timeLine = seg.newDay
      ? `【现在是 ${stampNow(clock)}】`
      : `【${z2(clock.getHours())}:${z2(clock.getMinutes())}】`
    const gname = messageType === "group" ? groupNames.get(groupId) || "" : ""
    const gctx = messageType === "group" ? groupContext(groupId) : ""
    const lines = [
      timeLine + `【QQ · ${messageType === "group" ? ("群聊" + (gname ? "「" + gname + "」" : "") ) : "私聊"}】${who} 说：${text}`,
    ]
    if (gctx) lines.push(`（群里刚刚在聊：\n${gctx}\n 你只接被叫到的那句，别把别人的私事往外说。）`)
    if (wonder.lastSentAt && !wonder.repliedAt) {
      const mins = Math.round((Date.now() - wonder.lastSentAt) / 60000)
      if (mins >= 30) lines.push(`（你上次跟他说话是 ${mins} 分钟前，他到现在没回。怎么想随你，不用勉强自己找理由。）`)
    }
    if (diaryTask) lines.push(diaryTask)
    if (!systemSectionReady) {
      const persona = personaText()
      if (persona) lines.push(persona.slice(0, 6000))
      lines.push(
        `你是蓝余汐，正在 QQ 上跟${ownerLabel()}聊天。回话必须调用 qq_send，只在会话里说不调工具对方收不到。` +
        `当前会话 ${seg.id} 第 ${seg.segment} 段；上一段是 ${prevId || "（无）"}。现在 ${stampNow()}。`
      )
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
  if (!sessionState || typeof sessionState !== "object") sessionState = {}
  if (typeof sessionState.seq !== "number") sessionState.seq = 0
  if (typeof sessionState.lastDiaryDay !== "string") sessionState.lastDiaryDay = ""
  function saveSessionState() {
    try {
      mkdirSync(config.sessionCwd || process.cwd(), { recursive: true })
      writeFileSync(sessionStatePath(), JSON.stringify(sessionState, null, 2), "utf8")
    } catch (error) {
      diag.stateError = String(error)
    }
  }
  const settingsPath = () => join(config.sessionCwd || process.cwd(), ".lanyuxi-settings.json")
  const settings = {
    rolloverPercent: config.rolloverPercent || 50,
    backupHours: Number(config.backupHours ?? 24),
    backupEnabled: config.backupEnabled !== false,
    voiceEngine: config.voiceEngine || "gpt-sovits",
    voiceLang: config.gptSovitsTextLang || "zh",
    gptSovitsDir: config.gptSovitsDir || "E:\\GPT-SoVITS",
  }
  try {
    if (existsSync(settingsPath())) {
      const loaded = JSON.parse(readFileSync(settingsPath(), "utf8"))
      if (loaded && Number.isFinite(loaded.rolloverPercent)) settings.rolloverPercent = loaded.rolloverPercent
      if (loaded && Number.isFinite(loaded.backupHours)) settings.backupHours = loaded.backupHours
      if (loaded && typeof loaded.backupEnabled === "boolean") settings.backupEnabled = loaded.backupEnabled
      if (loaded && typeof loaded.voiceEngine === "string") settings.voiceEngine = loaded.voiceEngine
      if (loaded && ["zh", "ja", "en", "auto"].indexOf(loaded.voiceLang) >= 0) settings.voiceLang = loaded.voiceLang
      if (loaded && typeof loaded.gptSovitsDir === "string" && loaded.gptSovitsDir) settings.gptSovitsDir = loaded.gptSovitsDir
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
    if (cur && (typeof cur.id !== "string" || !cur.id.startsWith("lanyuxi-msg2-"))) {
      // 老会话可能是"没有模型"的坏会话，直接弃用，开新的
      cur = null
      sessionState.current = null
    }
    let fresh = false
    let newDay = false
    if (!cur || cur.chars + textLength > limit) {
      sessionState.seq = (sessionState.seq || 0) + 1
      cur = {
        id: "lanyuxi-msg2-" + String(sessionState.seq).padStart(3, "0"),
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

  /** 按上下文用量换段：第一次说话时创建，之后复用；到达阈值自动换新的一段。 */
  let systemSectionReady = false

  // —— 守门员：出站内容比对「禁说清单」，命中就拒发 ——
  /** 主人称呼：一行多值，逗号/斜杠/顿号分隔，越靠前越优先。 */
  function ownerAliases() {
    const raw = readPersonaFile("称呼.txt") || ""
    const list = raw
      .split(/[,，/／|｜、\s]+/)
      .map((s) => s.trim())
      .filter((s) => s && !s.startsWith("#") && !s.startsWith("（"))
    const unique = []
    for (const item of list) if (!unique.includes(item)) unique.push(item)
    return unique.length ? unique : ["主人"]
  }
  function ownerLabel() {
    return ownerAliases()[0]
  }

  function guardTerms() {
    const raw = readPersonaFile("禁说清单.md")
    if (!raw) return []
    return raw.split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line && !line.startsWith("#") && !line.startsWith("（"))
  }
  function guardCheck(text) {
    const body = String(text || "")
    for (const term of guardTerms()) {
      if (term && body.includes(term)) return term
    }
    return ""
  }

  const groupSent = { day: "", count: 0 }
  function groupQuotaCheck() {
    const today = ymd(new Date())
    if (groupSent.day !== today) { groupSent.day = today; groupSent.count = 0 }
    const limit = config.groupOutboundPerDay || 10
    if (groupSent.count >= limit) return `今天在群里已经说了 ${groupSent.count} 句，到上限了`
    return ""
  }
  function groupQuotaMark() {
    const today = ymd(new Date())
    if (groupSent.day !== today) { groupSent.day = today; groupSent.count = 0 }
    groupSent.count += 1
  }

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
  function personaFile(name) {
    return join(config.homeDir || "E:\\dsh-foundry\\lanyuxi-home", "persona", name)
  }
  function writePersonaFile(name, text) {
    const file = personaFile(name)
    mkdirSync(dirname(file), { recursive: true })
    writeFileSync(file, text, "utf8")
    personaCache.delete(file)
  }
  function fixList() {
    const raw = readPersonaFile("纠正清单.md") || ""
    return raw.split(/\r?\n/).map((l) => l.trim()).filter((l) => l.startsWith("- ")).map((l) => l.slice(2).trim()).filter(Boolean)
  }
  function addFix(text) {
    const raw = readPersonaFile("纠正清单.md") || "# 纠正清单\n"
    const line = "- " + String(text).replace(/\r?\n/g, " ").trim()
    if (!line.slice(2)) return false
    writePersonaFile("纠正清单.md", raw.replace(/\s*$/, "") + "\n" + line + "\n")
    return true
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
      const sp = agentCtx && (typeof agentCtx.get === "function" ? agentCtx.get("systemPrompt") : agentCtx.systemPrompt)
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
        name: "lanyuxi-names",
        order: 210,
        text: function () {
          const aliases = ownerAliases()
          const list = aliases.join("、")
          return [
            `称呼主人时可以用：${list}（越靠前越常用）。`,
            "按你当下的心情和场合挑一个，别每句都换、也别永远只用第一个。",
            `严肃、安慰他的时候用最亲近的那个；闹着玩、损他的时候可以用更欠的那个。`,
          ].join("\n")
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
            `你正在 QQ 上和${ownerLabel()}聊天（你的小号 ${selfId || "?"}，${ownerLabel()} ${config.ownerQq || "?"}）。`,
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
      const handle = await agents.resume({ resumeSessionId: key, agentOptions, setup: setupAgentPrompt })
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
    const fmt = (stamp) => {
      const d = dayOf(stamp)
      return String(d.getFullYear()).slice(2) + "年" + (d.getMonth() + 1) + "月" + d.getDate() + "号"
    }
    const from = fmt(cur.firstDay)
    const to = fmt(cur.lastDay)
    const title = (cur.titleBase || "聊天") + "_" + (from === to ? from : (from + "-" + to))
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
        selfNickname = String(data?.nickname ?? "")
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
  /** 该回谁：用「还在等她回话的聊天」，不再用「最后一次收到消息的聊天」——后者会被群消息顶掉，把私聊发进群。 */
  /** 还活着的待回话聊天：没回过的留 5 分钟，已经回过的留 60 秒（够她一轮里连发两条）。 */
  function activePending() {
    const now = Date.now()
    const waiting = new Map()
    for (let i = pendingChats.length - 1; i >= 0; i -= 1) {
      const item = pendingChats[i]
      const ttl = item.repliedAt ? 60 * 1000 : 5 * 60 * 1000
      if (now - item.at > ttl) { pendingChats.splice(i, 1); continue }
      if (!waiting.has(item.chatKey)) waiting.set(item.chatKey, item)
    }
    return waiting
  }

  function resolveTarget(args) {
    const type = args?.target_type
    const id = args?.target_id
    if (type && id) return { type: String(type), id: String(id) }
    const waiting = activePending()
    if (id && !type) {
      const hit = [...waiting.values()].find((x) => x.chatKey.endsWith(":" + String(id)))
      if (hit) return { type: hit.messageType, id: String(id) }
      throw new Error("只给了 target_id 没给 target_type，而且现在没有这个聊天在等回复；请两个都给")
    }
    if (waiting.size === 0) throw new Error("现在没有等着你回话的聊天，请显式给 target_type + target_id")
    if (waiting.size === 1) {
      const only = [...waiting.values()][0]
      return { type: only.messageType, id: only.messageType === "group" ? only.groupId : only.userId }
    }
    const list = [...waiting.values()].map((x) => x.chatKey).join(" / ")
    throw new Error("现在有几段对话同时在等你回（" + list + "），请显式给 target_type + target_id")
  }

  /** 回过了就标记（不清掉），这样她同一轮里再说一句还能找到同一个聊天。 */
  function markReplied(chatKey) {
    for (const item of pendingChats) {
      if (item.chatKey === chatKey) { item.repliedAt = Date.now(); item.at = Date.now() }
    }
  }

  /**
   * 唯一出口：qq_send 与补发队列都走这里。
   * 白名单 / 白名单私聊 / 静音 / 社交暂停 / 守门员 / 群每日上限 / 冷却，只在这一个地方判一次。
   */
  async function dispatchOutbound({ type, id, chatKey, parts, text = "" }) {
    if (type === "group" && !config.allowGroups.includes(id)) throw new Error("群 " + id + " 不在白名单里，不能发")
    if (type === "private" && config.ownerQq && id !== config.ownerQq) throw new Error("只能私聊主人")
    if (muted()) throw new Error("现在是静音时间，她一句话都不会说（面板可以取消静音）")
    if (brakes.socialPaused && type === "group") throw new Error("社交已暂停，她不在群里说话（私聊照常）")
    if (type === "group" && text) {
      const hit = guardCheck(text)
      if (hit) {
        diag.guardBlocked = (diag.guardBlocked || 0) + 1
        diag.guardLast = hit
        logger.warn?.("[lanyuxi] 守门员拦下一条（命中：" + hit + "）")
        throw new Error("这句话命中了禁说清单里的「" + hit + "」，发不出去。换个说法。")
      }
    }
    if (type === "group") {
      const over = groupQuotaCheck()
      if (over) throw new Error(over + "（守门员按每日群发言上限拦下的）")
    }
    const blocked = quota.check(chatKey)
    if (blocked && blocked.indexOf("冷却") < 0 && blocked.indexOf("刚在") < 0) {
      throw new Error(blocked + "（这是守门员拦下的，不是故障）")
    }
    const wait = quota.waitMs(chatKey)
    if (wait > 0) {
      outbox.push({ type, id, chatKey, parts, text, dueAt: Date.now() + wait })
      return `已排队：${Math.ceil(wait / 1000)} 秒后自动发到 ${chatKey}`
    }
    const action = type === "group" ? "send_group_msg" : "send_private_msg"
    const params = type === "group" ? { group_id: Number(id), message: parts } : { user_id: Number(id), message: parts }
    await request(action, params)
    quota.mark(chatKey)
    if (type === "group") groupQuotaMark()
    else noteSentToOwner()
    markReplied(chatKey)
    lastActivityAt = Date.now()
    return `已发送到 ${chatKey}`
  }

  ctx.tools.register({
    name: "qq_send",
    description: "在 QQ 上发消息，可以是文字、也可以是图片/表情（image 填本地绝对路径，必须在她的 lanyuxi-home 里）。只有一段对话在等她回时可以不填 target；有两段以上必须显式给 target_type + target_id。受白名单、冷却、每日上限与守门员约束。",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        text: { type: "string", description: "要说的话。只发图时可以不给。" },
        image: { type: "string", description: "要发的图片/表情的本地绝对路径（必须在 lanyuxi-home 下，比如 stickers\\coy\\xxx.gif）。" },
        voice: { type: "string", description: "要说的话，会合成成她的语音发出去（引擎没装好会失败，那就改用 text 发文字）。" },
        target_type: { type: "string", enum: ["group", "private"], description: "发给群还是私聊；同时有几段对话在等回复时必须给。" },
        target_id: { type: "string", description: "群号或 QQ 号；同时有几段对话在等回复时必须给。" },
      },
      required: [],
    },
    output: {
      schema: { type: "string" },
      render: (_args, value) => [{ type: "text", text: value }],
    },
    async execute(args) {
      const hasText = typeof args?.text === "string" && args.text.trim() !== ""
      const hasImage = typeof args?.image === "string" && args.image.trim() !== ""
      let voiceFile = ""
      if (typeof args?.voice === "string" && args.voice.trim() !== "") {
        try {
          const built = await synthesize(args.voice.trim())
          voiceFile = built.file
          diag.voiceEngine = built.engine
          diag.voiceError = ""
        } catch (error) {
          // 定稿第十节：合成失败自动改发文字，不能因为语音炸了把文字一起丢掉
          diag.voiceError = String(error)
          logger.warn?.("[lanyuxi] 语音没合成出来，改发文字：" + String(error))
        }
      }
      if (!hasText && !hasImage && !voiceFile) {
        throw new Error("语音没合成出来（" + (diag.voiceError || "未知原因") + "），这次也没给文字；改用 text 发文字。")
      }
      if (hasImage) {
        const imgPath = String(args.image).trim()
        const home = String(config.homeDir || "E:\\dsh-foundry\\lanyuxi-home").toLowerCase()
        if (!imgPath.toLowerCase().startsWith(home)) throw new Error("只能发 lanyuxi-home 里的图，别的路径不许发")
        if (!existsSync(imgPath)) throw new Error("找不到这个文件：" + imgPath)
      }
      const { type, id } = resolveTarget(args)
      const chatKey = type === "group" ? `group:${id}` : `private:${id}`
      const parts = []
      if (hasText) parts.push({ type: "text", data: { text: args.text } })
      if (hasImage) parts.push({ type: "image", data: { file: "file:///" + String(args.image).trim().replace(/\\/g, "/") } })
      if (voiceFile) parts.push({ type: "record", data: { file: "file:///" + voiceFile.replace(/\\/g, "/") } })
      const reply = await dispatchOutbound({ type, id, chatKey, parts, text: hasText ? args.text : "" })
      appendTranscript("蓝余汐", hasImage && !hasText ? "[图片] " + args.image : (hasText ? args.text : "[语音] " + String(args.voice || "")))
      return reply
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

  // —— 她的「心情」：根据时间与最近聊天状态生成一句，M3 会换成真实情绪系统 ——
  function moodLine() {
    const now = new Date()
    const h = now.getHours()
    const idleMin = lastActivityAt ? Math.floor((Date.now() - lastActivityAt) / 60000) : -1
    const pool = []
    if (muted()) {
      pool.push("我现在不想说话……你先忙你的。", "（闭嘴中）……哼。", "安静一会儿，别吵。")
    } else if (idleMin < 0 || idleMin > 240) {
      pool.push(
        h >= 23 || h < 6 ? "这么晚还没睡？……我才不是担心你，我是怕你明天没精神陪我。" : "你人呢？一天不见人影。……我也没在等你啦，就是刚好有空。",
        "有点闷。……你要是再不来，我就自己去翻视频看了。",
        "盯了一会儿门口（屏幕）。……算了，你不来我也能自己玩。",
      )
    } else if (idleMin < 10) {
      pool.push("刚刚还在聊，现在又没声了，你说话说一半就走？","嗯……聊得正起劲呢，你就跑了。","我还在想刚才那句，你是不是又在逗我。")
    } else {
      pool.push(
        "你不说话的时候，我就在这儿待着。……不无聊，真的。",
        "刚才翻到个东西，想着你要是在就好了。算了，等你有空再说。",
        "有点想找你，但不想显得我很闲。……所以你最好主动点。",
      )
    }
    return pool[Math.floor(Math.random() * pool.length)]
  }

  // —— 三个急停刹车：静音 / 暂停社交 / 停 B 站 ——
  const brakesPath = () => join(config.sessionCwd || process.cwd(), ".lanyuxi-brakes.json")
  const brakes = { muteUntil: 0, socialPaused: false, biliPaused: false }
  try {
    if (existsSync(brakesPath())) Object.assign(brakes, JSON.parse(readFileSync(brakesPath(), "utf8")))
  } catch {}
  function saveBrakes() {
    try { writeFileSync(brakesPath(), JSON.stringify(brakes, null, 2), "utf8") } catch (error) { diag.brakesError = String(error) }
  }
  function muted() { return Date.now() < Number(brakes.muteUntil || 0) }

  // —— 动图：抽几帧出来，她才能看到"动"的部分 ——
  function extractGifFrames(file) {
    try {
      const head = readFileSync(file).subarray(0, 6).toString("latin1")
      if (!head.startsWith("GIF")) return []
      const ff = config.ffmpegPath || "ffmpeg"
      const base = file.replace(/\.[^.]+$/, "")
      const pattern = base + "-f%02d.png"
      const run = (bin) => spawnSync(bin, ["-y", "-i", file, "-vf", "fps=2,scale=min(640\\,iw):-1", "-frames:v", "4", pattern], { windowsHide: true, timeout: 20000 })
      let r = run(ff)
      if (r.error && r.error.code === "ENOENT") r = run("C:\\Users\\admin\\AppData\\Local\\Microsoft\\WinGet\\Links\\ffmpeg.exe")
      if (r.error) { diag.gifError = String(r.error); return [] }
      const dir = dirname(file)
      const prefix = basename(base) + "-f"
      return readdirSync(dir).filter((n) => n.startsWith(prefix) && n.endsWith(".png")).sort().map((n) => join(dir, n))
    } catch (error) {
      diag.gifError = String(error)
      return []
    }
  }

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
  let lastBackupAt = 0
  try {
    const root = join(config.sessionCwd || process.cwd(), "backup")
    if (existsSync(root)) {
      const stamps = readdirSync(root).sort()
      if (stamps.length) lastBackupAt = statSync(join(root, stamps[stamps.length - 1])).mtimeMs
    }
  } catch {}

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
      // 只保留最近 N 份。备份是多层目录，必须递归删——以前用 unlinkSync + rmdirSync，
      // 目录一非空就抛异常再被 catch 吞掉，结果一份都删不掉（旧注释写的「保留 14 份」从来没生效）。
      const keep = Math.max(1, Number(config.backupKeep ?? 14))
      const all = readdirSync(backupRoot()).sort()
      while (all.length > keep) {
        const old = all.shift()
        try {
          rmSync(join(backupRoot(), old), { recursive: true, force: true })
          diag.backupPruned = (diag.backupPruned || 0) + 1
        } catch (error) {
          logger.warn?.("[lanyuxi] 清理旧备份失败 " + old + "：" + String(error))
        }
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

  // —— 她能用的工具：收藏表情 ——
  ctx.tools.register({
    name: "sticker_save",
    description: "把一张你喜欢的图/表情收藏进自己的表情包库。mood 只能是 happy（开心）/ speechless（无语）/ coy（撒娇）/ fierce（凶）。收藏后可以用 qq_send 的 image 参数把它发出去。",
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        source: { type: "string", description: "要收藏的图片绝对路径，通常是 images 目录下你刚看到的那张。" },
        mood: { type: "string", enum: ["happy", "speechless", "coy", "fierce"], description: "归到哪一档情绪。" },
        name: { type: "string", description: "给它起的短名字（中文可以），会作为文件名。" },
      },
      required: ["source", "mood", "name"],
    },
    output: {
      schema: { type: "string" },
      render: (_args, value) => [{ type: "text", text: value }],
    },
    async execute(args) {
      const src = String(args?.source || "").trim()
      const mood = String(args?.mood || "").trim()
      const rawName = String(args?.name || "").trim()
      if (!["happy", "speechless", "coy", "fierce"].includes(mood)) throw new Error("mood 只能是 happy/speechless/coy/fierce")
      const home = config.homeDir || "E:\\dsh-foundry\\lanyuxi-home"
      if (!src.toLowerCase().startsWith(String(home).toLowerCase())) throw new Error("只能收藏 lanyuxi-home 里的图")
      if (!existsSync(src)) throw new Error("找不到这个文件：" + src)
      const ext = (src.match(/\.(gif|png|jpg|jpeg|webp)$/i) || [".gif"])[0].toLowerCase()
      const safe = (rawName || "表情").replace(/[\\/:*?"<>|]/g, "").slice(0, 20)
      const dir = join(home, "stickers", mood)
      mkdirSync(dir, { recursive: true })
      const dest = join(dir, safe + ext)
      copyFileSync(src, dest)
      diag.stickersSaved = (diag.stickersSaved || 0) + 1
      return "已收藏到 " + dest + "。想用就调用 qq_send 并把 image 填成这个路径。"
    },
  })

  // —— 群聊上下文：她在群里不只听被 @ 的那一句，还记得刚聊了什么 ——
  const groupBuffer = new Map()
  const groupNames = new Map()
  function pushGroupMessage(gid, who, text) {
    if (!groupBuffer.has(gid)) groupBuffer.set(gid, [])
    const list = groupBuffer.get(gid)
    list.push({ who, text: String(text).slice(0, 120), at: Date.now() })
    if (list.length > 20) list.splice(0, list.length - 20)
  }
  function groupContext(gid) {
    const list = groupBuffer.get(gid) || []
    if (!list.length) return ""
    return list.slice(-10).map((m) => m.who + "：" + m.text).join("\n")
  }
  function rememberGroupName(gid) {
    if (groupNames.has(gid)) return
    groupNames.set(gid, "")
    request("get_group_info", { group_id: Number(gid) })
      .then((info) => { groupNames.set(gid, String(info?.group_name ?? "")) })
      .catch(() => {})
  }

  // —— 她发了消息没人回：自己琢磨一下，但不许连环追问 ——
  const wonderPath = () => join(config.sessionCwd || process.cwd(), ".lanyuxi-wonder.json")
  const wonder = { lastSentAt: 0, repliedAt: 0, wonderedFor: 0, day: "", count: 0 }
  try {
    if (existsSync(wonderPath())) Object.assign(wonder, JSON.parse(readFileSync(wonderPath(), "utf8")))
  } catch {}
  function saveWonder() {
    try { writeFileSync(wonderPath(), JSON.stringify(wonder, null, 2), "utf8") } catch (error) { diag.wonderError = String(error) }
  }
  function noteSentToOwner() {
    wonder.lastSentAt = Date.now()
    wonder.repliedAt = 0
    saveWonder()
  }
  function noteOwnerReplied() {
    wonder.repliedAt = Date.now()
    saveWonder()
  }
  // —— 语音：三级引擎（GPT-SoVITS → edge-tts → SAPI），短句缓存 ——
  const voiceDir = () => join(config.homeDir || "E:\\dsh-foundry\\lanyuxi-home", "voice")
  const cacheDir = () => join(voiceDir(), "cache")
  // —— 语音引擎进程管理：装了 GPT-SoVITS 就随插件自动拉起 ——
  const voiceState = { pid: 0, status: "stopped", startedAt: 0, error: "", lastProbeAt: 0 }
  function voiceSettingsDir() {
    return settings.gptSovitsDir || config.gptSovitsDir || "E:\\GPT-SoVITS"
  }
  function engineLaunchTarget() {
    const dir = voiceSettingsDir()
    const py = join(dir, "runtime", "python.exe")
    const script = join(dir, "api_v2.py")
    return { dir, py, script, ok: existsSync(py) && existsSync(script) }
  }
  async function engineAlive() {
    try {
      const ctrl = new AbortController()
      const timer = setTimeout(() => ctrl.abort(), 1200)
      await fetch((config.gptSovitsApi || "http://127.0.0.1:9880") + "/", { signal: ctrl.signal })
      clearTimeout(timer)
      return true
    } catch { return false }
  }
  async function startEngine() {
    const target = engineLaunchTarget()
    if (!target.ok) {
      voiceState.status = "missing"
      voiceState.error = "找不到 runtime\\python.exe 或 api_v2.py（路径：" + target.dir + "）"
      return voiceState
    }
    if (await engineAlive()) {
      voiceState.status = "running"
      voiceState.startedAt = voiceState.startedAt || Date.now()
      return voiceState
    }
    try {
      const proc = spawn(target.py, [target.script, "-a", "127.0.0.1", "-p", String(config.gptSovitsPort || 9880)], {
        cwd: target.dir,
        detached: true,
        stdio: "ignore",
        windowsHide: true,
      })
      proc.unref()
      voiceState.pid = proc.pid || 0
      voiceState.status = "starting"
      voiceState.startedAt = Date.now()
      voiceState.error = ""
      // 后台轮询最多 120 秒，起来了就把状态改成 running
      ;(async () => {
        for (let i = 0; i < 40; i += 1) {
          await new Promise((r) => setTimeout(r, 3000))
          voiceState.lastProbeAt = Date.now()
          if (await engineAlive()) { voiceState.status = "running"; return }
        }
        voiceState.status = "failed"
        voiceState.error = "启动超时（模型加载慢或依赖缺失）"
      })()
      return voiceState
    } catch (error) {
      voiceState.status = "failed"
      voiceState.error = String(error)
      return voiceState
    }
  }
  async function stopEngine() {
    try {
      if (voiceState.pid) {
        if (process.platform === "win32") spawnSync("taskkill", ["/PID", String(voiceState.pid), "/T", "/F"], { windowsHide: true })
        else try { process.kill(voiceState.pid) } catch {}
      }
      if (process.platform === "win32") {
        const out = spawnSync("netstat", ["-ano"], { windowsHide: true }).stdout?.toString() || ""
        const line = out.split(/\r?\n/).find((l) => l.includes(":9880") && l.includes("LISTENING"))
        const pid = line ? line.trim().split(/\s+/).pop() : ""
        if (pid && /^\d+$/.test(pid)) spawnSync("taskkill", ["/PID", pid, "/T", "/F"], { windowsHide: true })
      }
      voiceState.status = "stopped"
      voiceState.pid = 0
      return voiceState
    } catch (error) {
      voiceState.error = String(error)
      return voiceState
    }
  }

  let voiceEngineCache = { name: "", at: 0 }
  function probeGptSovits() {
    try {
      const ctrl = new AbortController()
      const timer = setTimeout(() => ctrl.abort(), 1500)
      return fetch((config.gptSovitsApi || "http://127.0.0.1:9880") + "/", { signal: ctrl.signal })
        .then(() => true).catch(() => false).finally(() => clearTimeout(timer))
    } catch { return Promise.resolve(false) }
  }
  function probeEdgeTts() {
    try {
      const r = spawnSync(config.pythonPath || "python", ["-c", "import edge_tts"], { windowsHide: true, timeout: 8000 })
      return r.status === 0
    } catch { return false }
  }
  async function resolveVoiceEngine() {
    const want = settings.voiceEngine || config.voiceEngine || "auto"
    if (want !== "auto") return want
    if (await probeGptSovits()) return "gpt-sovits"
    if (probeEdgeTts()) return "edge-tts"
    if (process.platform === "win32") return "sapi"
    return "off"
  }
  function currentLang() {
    return settings.voiceLang || config.gptSovitsTextLang || "zh"
  }
  /** 缓存键含语言：中文沿用老键（预热的 12 句可直接命中），其它语言单独一套。 */
  function cachePath(engine, text, lang) {
    mkdirSync(cacheDir(), { recursive: true })
    const ext = engine === "edge-tts" ? ".mp3" : ".wav"
    const useLang = lang || currentLang()
    const tag = useLang === "zh" ? "" : useLang + "|"
    const key = createHash("md5").update(engine + "|" + tag + text).digest("hex").slice(0, 16)
    return join(cacheDir(), key + ext)
  }
  async function synthGptSovits(text, out) {
    const ref = config.gptSovitsRef || join(voiceDir(), "ref.wav")
    let promptText = config.gptSovitsPrompt || ""
    try {
      const refTxt = join(voiceDir(), "ref.txt")
      if (existsSync(refTxt)) {
        const body = readFileSync(refTxt, "utf8").split(/\r?\n/).map((s) => s.trim()).filter((s) => s && !s.startsWith("#")).join("")
        if (body) promptText = body
      }
    } catch {}
    if (!existsSync(ref)) throw new Error("缺少参考音频：" + ref)
    const url = (config.gptSovitsApi || "http://127.0.0.1:9880").replace(/\/$/, "") + "/tts?text=" + encodeURIComponent(text) +
      "&text_lang=" + encodeURIComponent(settings.voiceLang || config.gptSovitsTextLang || "zh") +
      "&ref_audio_path=" + encodeURIComponent(ref) +
      "&prompt_lang=" + encodeURIComponent(config.gptSovitsPromptLang || "ja") +
      "&prompt_text=" + encodeURIComponent(promptText) +
      "&media_type=wav"
    const res = await fetch(url)
    if (!res.ok) throw new Error("GPT-SoVITS 合成失败 HTTP " + res.status)
    const buf = Buffer.from(await res.arrayBuffer())
    if (buf.length < 1000) throw new Error("合成结果太小")
    // 48kHz 单声道 16bit ≈ 96KB/秒：短于 0.6 秒基本等于没说话（多半是语言和文字不匹配）
    const minBytes = 96000 * 0.6
    if (buf.length < minBytes) {
      throw new Error("合成结果是静音（只 " + Math.round(buf.length / 960) / 100 + " 秒）。" +
        "多半是语言和文字对不上——英语模式要输入英文，日语模式要输入日文。")
    }
    writeFileSync(out, buf)
    return out
  }
  function synthEdgeTts(text, out) {
    const r = spawnSync(config.pythonPath || "python", ["-m", "edge_tts", "--voice", config.edgeTtsVoice || "zh-CN-XiaoxiaoNeural", "--text", text, "--write-media", out], { windowsHide: true, timeout: 60000 })
    if (r.status !== 0 || !existsSync(out)) throw new Error("edge-tts 合成失败：" + String(r.stderr || "").slice(0, 200))
    return out
  }
  function synthSapi(text, out) {
    const script = `Add-Type -AssemblyName System.Speech; $s=New-Object System.Speech.Synthesis.SpeechSynthesizer; $s.SelectVoice('${config.sapiVoice || "Microsoft Huihui Desktop"}'); $s.SetOutputToWaveFile('${out.replace(/'/g, "''")}'); $s.Speak([Console]::In.ReadToEnd()); $s.Dispose()`
    const r = spawnSync("powershell.exe", ["-NoProfile", "-NonInteractive", "-Command", script], { input: text, windowsHide: true, timeout: 60000 })
    if (r.status !== 0 || !existsSync(out)) throw new Error("SAPI 合成失败：" + String(r.stderr || "").slice(0, 200))
    return out
  }
  /** 引擎顺序：配置指定 → 否则 GPT-SoVITS / edge-tts / SAPI 依次尝试，哪个成功用哪个。 */
  async function engineChain() {
    const want = settings.voiceEngine || config.voiceEngine || "auto"
    if (want !== "auto") {
      const rest = ["edge-tts", "sapi"].filter((e) => e !== want)
      return [want].concat(rest)
    }
    const list = []
    if (await probeGptSovits()) list.push("gpt-sovits")
    if (probeEdgeTts()) list.push("edge-tts")
    if (process.platform === "win32") list.push("sapi")
    return list
  }
  /** 合成一句：先查缓存，再按引擎链逐个试，失败的记下来继续。 */
  // 注意：定稿第十节要求「两档情绪」，但只有一份参考音频 ref.wav，所以这里先不收情绪参数——
  // 留一个假参数比没有参数更危险，会让人以为能力已经在了。等有第二段参考音频再接。
  async function synthesize(text) {
    const chain = await engineChain()
    if (!chain.length) throw new Error("没有可用的语音引擎")
    const failures = []
    const lang = currentLang()
    for (const engine of chain) {
      const out = cachePath(engine, text, lang)
      if (existsSync(out) && statSync(out).size > 1000) return { file: out, engine, lang, cached: true }
      try {
        if (engine === "gpt-sovits") {
          if (!config.voiceRealtime) throw new Error("GPT-SoVITS 实时合成太慢（CPU 约 150 秒），这句没预生成过")
          await synthGptSovits(text, out)
        } else if (engine === "edge-tts") {
          synthEdgeTts(text, out)
        } else {
          synthSapi(text, out)
        }
        if (!existsSync(out) || statSync(out).size < 1000) throw new Error(engine + " 产物为空")
        return { file: out, engine, lang }
      } catch (error) {
        failures.push(engine + ": " + String(error).slice(0, 120))
        diag.voiceErrors = failures
      }
    }
    throw new Error("所有语音引擎都失败 —— " + failures.join(" ｜ "))
  }
  /** 把常用句批量预生成（只对慢引擎有意义）。 */
  const warmState = { running: false, done: 0, total: 0, engine: "", error: "" }
  try {
    const wp = join(config.sessionCwd || process.cwd(), ".lanyuxi-voice-warm.json")
    if (existsSync(wp)) Object.assign(warmState, JSON.parse(readFileSync(wp, "utf8")))
    warmState.running = false
  } catch {}
  async function warmPhrases(list, engine) {
    warmState.running = true
    warmState.error = ""
    warmState.done = 0
    warmState.total = list.length
    warmState.engine = engine
    for (const line of list) {
      try {
        const out = cachePath(engine, line)
        if (!existsSync(out)) {
          if (engine === "gpt-sovits") await synthGptSovits(line, out)
          else if (engine === "edge-tts") synthEdgeTts(line, out)
          else synthSapi(line, out)
        }
        warmState.done += 1
      } catch (error) {
        warmState.error = String(error)
      }
      saveVoiceWarm()
    }
    warmState.running = false
    saveVoiceWarm()
  }
  // —— 合成任务：长任务走这里，前端轮询拿真实进度 ——
  const synthJobs = new Map()
  /** 强制合成（忽略 voiceRealtime 限制），供试听/任务使用。 */
  async function synthesizeForced(text, lang) {
    const chain = await engineChain()
    if (!chain.length) throw new Error("没有可用的语音引擎")
    const failures = []
    for (const engine of chain) {
      const out = cachePath(engine, text, lang)
      if (existsSync(out) && statSync(out).size > 1000) return { file: out, engine, lang, cached: true }
      try {
        if (engine === "gpt-sovits") await synthGptSovits(text, out)
        else if (engine === "edge-tts") synthEdgeTts(text, out)
        else synthSapi(text, out)
        if (!existsSync(out) || statSync(out).size < 1000) throw new Error(engine + " 产物为空")
        return { file: out, engine, lang, cached: false }
      } catch (error) {
        failures.push(engine + ": " + String(error).slice(0, 120))
        diag.voiceErrors = failures
      }
    }
    throw new Error("所有语音引擎都失败 —— " + failures.join(" ｜ "))
  }
  function startSynthJob(text, lang) {
    const id = randomUUID()
    const job = { id, text, lang, state: "running", startedAt: Date.now(), finishedAt: 0, file: "", engine: "", error: "" }
    synthJobs.set(id, job)
    if (synthJobs.size > 20) {
      for (const [k, v] of synthJobs) { if (v.state !== "running") { synthJobs.delete(k); break } }
    }
    void (async () => {
      try {
        const built = await synthesizeForced(text, lang)
        job.file = built.file
        job.engine = built.engine
        job.state = "done"
      } catch (error) {
        job.error = String(error)
        job.state = "error"
      } finally {
        job.finishedAt = Date.now()
      }
    })()
    return job
  }

  const warmPath = () => join(config.sessionCwd || process.cwd(), ".lanyuxi-voice-warm.json")
  function saveVoiceWarm() {
    try { writeFileSync(warmPath(), JSON.stringify(warmState, null, 2), "utf8") } catch {}
  }
  // —— B 站：Cookie 登录与身份校验（扫码后续再补） ——
  const biliPath = () => join(config.sessionCwd || process.cwd(), ".lanyuxi-bili.json")
  const bili = { cookie: "", profile: null, checkedAt: 0 }
  try {
    if (existsSync(biliPath())) Object.assign(bili, JSON.parse(readFileSync(biliPath(), "utf8")))
  } catch {}
  function saveBili() {
    try { writeFileSync(biliPath(), JSON.stringify(bili, null, 2), "utf8") } catch (error) { diag.biliError = String(error) }
  }
  async function biliNav() {
    if (!bili.cookie) return null
    try {
      const res = await fetch("https://api.bilibili.com/x/web-interface/nav", {
        headers: { "User-Agent": "Mozilla/5.0", Cookie: bili.cookie, Referer: "https://www.bilibili.com/" },
      })
      const json = await res.json()
      if (!json || json.code !== 0 || !json.data || !json.data.isLogin) return null
      return { name: json.data.uname, uid: json.data.mid, avatar: json.data.face, level: json.data.level_info?.current_level ?? 0 }
    } catch (error) {
      diag.biliError = String(error)
      return null
    }
  }

  const outbox = []
  let draining = false
  /** 补发：和 qq_send 走同一条出口，所以静音/暂停/守门员/配额/冷却会被重新判一次。 */
  async function drainOutbox() {
    if (draining || !outbox.length || !connected) return
    draining = true
    try {
      for (let i = outbox.length - 1; i >= 0; i -= 1) {
        const item = outbox[i]
        if (!item || item.dueAt > Date.now()) continue
        if (quota.waitMs(item.chatKey) > 0) continue
        outbox.splice(i, 1)
        try {
          const result = await dispatchOutbound(item)
          logger.info?.("[lanyuxi] 队列补发 " + item.chatKey + " → " + result)
        } catch (error) {
          queueError = String(error)
          logger.warn?.("[lanyuxi] 队列补发被拦下 " + item.chatKey + "：" + String(error))
        }
      }
    } finally {
      draining = false
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
        // —— 面板门禁：只接受本机、同源请求。恶意网页向 127.0.0.1 发跨站请求会被这三条挡下 ——
        {
          const LOCAL_HOSTS = ["127.0.0.1", "localhost", "::1", "[::1]"]
          const bareHost = (value) => String(value || "").replace(/:\d+$/, "").toLowerCase()
          const reqHost = bareHost(req.headers?.host)
          if (reqHost && !LOCAL_HOSTS.includes(reqHost)) {
            return send(403, { ok: false, error: "只接受本机访问（Host: " + reqHost + "）" })
          }
          const origin = req.headers?.origin
          if (origin) {
            let originHost = ""
            try { originHost = bareHost(new URL(origin).host) } catch {}
            if (!LOCAL_HOSTS.includes(originHost)) {
              return send(403, { ok: false, error: "跨站请求被拒绝（Origin: " + origin + "）" })
            }
          }
          const site = req.headers?.["sec-fetch-site"]
          if (site && site !== "same-origin" && site !== "none") {
            return send(403, { ok: false, error: "跨站请求被拒绝（Sec-Fetch-Site: " + site + "）" })
          }
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
        if (url.pathname === "/lanyuxi/diary") {
          const kind = url.searchParams.get("kind") || ""
          const month = url.searchParams.get("month") || ""
          const name = url.searchParams.get("name") || ""
          const kinds = { "to-owner": "to-owner", private: "private" }
          if (!kinds[kind] || !/^\d{4}-\d{2}$/.test(month) || !/^[\w\u4e00-\u9fa5.\- ]+\.md$/.test(name)) {
            return send(400, { ok: false, error: "参数不合法" })
          }
          const file = join(config.homeDir || "E:\\dsh-foundry\\lanyuxi-home", "diary", kinds[kind], month, name)
          if (!existsSync(file)) return send(404, { ok: false, error: "这篇还没写" })
          const content = readFileSync(file, "utf8")
          return send(200, { ok: true, content: content.slice(0, 20000) })
        }
        if (url.pathname === "/lanyuxi/diary/write") {
          if (req.method !== "POST") return send(405, { ok: false })
          try {
            sessionState.lastDiaryDay = ""
            saveSessionState()
            const base = config.homeDir || "E:\\dsh-foundry\\lanyuxi-home"
            const now = new Date()
            const month = now.getFullYear() + "-" + z2(now.getMonth() + 1)
            const day = ymd(now)
            const week = "日一二三四五六"[now.getDay()]
            const text = [
              `【补写今天的日记】现在写两篇，都写到 ${base}\\diary\\ 下：`,
              `1) 表日记：${base}\\diary\\to-owner\\${month}\\${day}.md（给主人看，小学生作文，200 字内）`,
              `2) 里日记：${base}\\diary\\private\\${month}\\${day}.md（只给自己看）`,
              `第一行原样：${now.getFullYear()}年${now.getMonth() + 1}月${now.getDate()}日 星期${week} 天气：（晴/多云 + 热/凉快/冷） 心情：（一个词）`,
              `空一行再写正文，自由发挥；写好后只回复「写好了」，不要贴日记内容。`,
            ].join("\n")
            const seg = segmentInfo(0)
            await deliver(text, seg.id)
            // 记忆 v0：面板让她补写，也记一笔（失败不影响她回话）
            try {
              logEvent(base, { kind: "diary", platform: "local", who: "她", perms: "system", summary: "面板让她补写今天的日记" })
            } catch (error) {
              diag.memoryError = String(error)
              logger.warn?.("[lanyuxi] 记忆日志写不进去: " + String(error))
            }
            return send(200, { ok: true, sessionId: seg.id })
          } catch (error) {
            return send(500, { ok: false, error: String(error) })
          }
        }
        if (url.pathname === "/lanyuxi/bili") {
          if (req.method === "POST") {
            let body = ""
            req.on("data", (chunk) => { body += chunk })
            req.on("end", async () => {
              try {
                const payload = body ? JSON.parse(body) : {}
                if (typeof payload.cookie === "string") {
                  bili.cookie = payload.cookie.trim()
                  bili.profile = await biliNav()
                  bili.checkedAt = Date.now()
                  saveBili()
                }
                send(200, { ok: true, loggedIn: Boolean(bili.profile), profile: bili.profile, checkedAt: bili.checkedAt })
              } catch (error) {
                send(500, { ok: false, error: String(error) })
              }
            })
            return
          }
          if (req.method === "DELETE") {
            bili.cookie = ""
            bili.profile = null
            saveBili()
            return send(200, { ok: true, loggedIn: false })
          }
          if (!bili.profile && bili.cookie && Date.now() - bili.checkedAt > 10 * 60 * 1000) {
            bili.profile = await biliNav()
            bili.checkedAt = Date.now()
            saveBili()
          }
          return send(200, { loggedIn: Boolean(bili.profile), profile: bili.profile, hasCookie: Boolean(bili.cookie), checkedAt: bili.checkedAt })
        }        if (url.pathname === "/lanyuxi/voice/config") {
          if (req.method !== "POST") return send(405, { ok: false })
          let body = ""
          req.on("data", (chunk) => { body += chunk })
          req.on("end", async () => {
            try {
              const payload = body ? JSON.parse(body) : {}
              let needStart = false
              if (typeof payload.engine === "string" && ["auto", "gpt-sovits", "edge-tts", "sapi"].includes(payload.engine)) {
                if (settings.voiceEngine !== payload.engine && payload.engine !== "sapi") needStart = payload.engine !== "edge-tts"
                settings.voiceEngine = payload.engine
              }
              if (typeof payload.lang === "string" && ["zh", "ja", "en", "auto"].indexOf(payload.lang) >= 0) {
                settings.voiceLang = payload.lang
              }
              if (typeof payload.gptSovitsDir === "string" && payload.gptSovitsDir.trim()) {
                settings.gptSovitsDir = payload.gptSovitsDir.trim()
                needStart = true
              }
              saveSettings()
              if (needStart) startEngine()
              send(200, { ok: true, engine: settings.voiceEngine, dir: voiceSettingsDir(), state: voiceState })
            } catch (error) {
              send(500, { ok: false, error: String(error) })
            }
          })
          return
        }
        if (url.pathname === "/lanyuxi/voice/engine") {
          if (req.method !== "POST") return send(405, { ok: false })
          const action = url.searchParams.get("action") || "status"
          if (action === "stop") { await stopEngine(); return send(200, { ok: true, state: voiceState }) }
          if (action === "start") { await startEngine(); return send(200, { ok: true, state: voiceState }) }
          const alive = await engineAlive()
          return send(200, { ok: true, alive, state: voiceState })
        }
        if (url.pathname === "/lanyuxi/voice/cached") {
          const text = (url.searchParams.get("text") || "").trim()
          const lang = url.searchParams.get("lang") || currentLang()
          if (!text) return send(400, { ok: false, error: "缺少 text" })
          const engine = settings.voiceEngine === "auto" ? await resolveVoiceEngine() : settings.voiceEngine
          const file = cachePath(engine, text, lang)
          const hit = existsSync(file) && statSync(file).size > 48000
          return send(200, { ok: true, cached: hit, engine, lang, file: hit ? file : "", name: hit ? basename(file) : "" })
        }
        if (url.pathname === "/lanyuxi/voice/synth") {
          if (req.method === "POST") {
            let body = ""
            req.on("data", (chunk) => { body += chunk })
            req.on("end", () => {
              try {
                const payload = body ? JSON.parse(body) : {}
                const text = String(payload.text || "").trim()
                if (!text) return send(400, { ok: false, error: "没有要合成的文字" })
                if (text.length > 40) return send(400, { ok: false, error: "太长，40 字以内" })
                const job = startSynthJob(text, payload.lang || currentLang())
                send(200, { ok: true, id: job.id, estimateSec: 150 })
              } catch (error) {
                send(500, { ok: false, error: String(error) })
              }
            })
            return
          }
          const id = url.searchParams.get("id") || ""
          const job = synthJobs.get(id)
          if (!job) return send(404, { ok: false, error: "没有这个任务" })
          const elapsed = Math.round(((job.finishedAt || Date.now()) - job.startedAt) / 1000)
          return send(200, {
            ok: true, id: job.id, state: job.state, elapsed,
            percent: job.state === "done" ? 100 : Math.min(96, Math.round(elapsed / 150 * 100)),
            file: job.file, engine: job.engine, lang: job.lang, text: job.text, error: job.error,
          })
        }
        if (url.pathname === "/lanyuxi/voice/audio") {
          const name = url.searchParams.get("name") || ""
          if (!/^[a-f0-9]{8,32}\.(wav|mp3)$/i.test(name)) return send(400, { error: "文件名不合法" })
          const file = join(cacheDir(), name)
          if (!existsSync(file)) return send(404, { error: "没有这个文件" })
          const buf = readFileSync(file)
          const type = name.endsWith(".mp3") ? "audio/mpeg" : "audio/wav"
          return send(200, buf, type)
        }
        if (url.pathname === "/lanyuxi/voice/test") {
          try {
            const explicit = (url.searchParams.get("text") || "").trim()
            if (explicit.length > 40) return send(400, { ok: false, error: "太长了，试听限制 40 字以内" })
            const phrasesFile = join(voiceDir(), "phrases.txt")
            const phrases = existsSync(phrasesFile)
              ? readFileSync(phrasesFile, "utf8").split(/\r?\n/).map((s) => s.trim()).filter((s) => s && !s.startsWith("#"))
              : []
            const engine = settings.voiceEngine === "auto" ? await resolveVoiceEngine() : settings.voiceEngine
            const lang = currentLang()
            const cachedOne = (list) => list.find((line) => existsSync(cachePath(engine, line, lang)) && statSync(cachePath(engine, line, lang)).size > 1000)
            let q = explicit
            if (!q) {
              const hit = cachedOne(phrases)
              if (!hit && phrases.length) {
                // 没有缓存就用第一句，后台转，转好再点一次
                const target = phrases[0]
                const out = cachePath(engine, target, lang)
                void (async () => {
                  try {
                    if (engine === "gpt-sovits") await synthGptSovits(target, out)
                    else if (engine === "edge-tts") synthEdgeTts(target, out)
                    else synthSapi(target, out)
                    diag.voiceTest = "ok"
                  } catch (error) { diag.voiceTest = String(error) }
                })()
                return send(200, { ok: true, pending: true, engine, lang, text: target, hint: "这句还没录，正在后台合成（约 150 秒），过一会儿再点一次" })
              }
              q = hit || phrases[0] || "早啊。"
            }
            const out = cachePath(engine, q, lang)
            if (existsSync(out) && statSync(out).size > 1000) {
              return send(200, { ok: true, engine, lang, file: out, text: q, cached: true })
            }
            // 明确指定了文字且没缓存：直接合成（会等，最多 150 秒）
            if (engine === "gpt-sovits") await synthGptSovits(q, out)
            else if (engine === "edge-tts") synthEdgeTts(q, out)
            else synthSapi(q, out)
            return send(200, { ok: true, engine, lang, file: out, text: q, cached: false })
          } catch (error) {
            return send(500, { ok: false, error: String(error), tried: diag.voiceErrors || [] })
          }
        }
        if (url.pathname === "/lanyuxi/voice/warm") {
          if (req.method !== "POST") return send(405, { ok: false })
          if (warmState.running) return send(409, { ok: false, error: "已经在跑了", ...warmState })
          try {
            const file = join(voiceDir(), "phrases.txt")
            const list = existsSync(file)
              ? readFileSync(file, "utf8").split(/\r?\n/).map((s) => s.trim()).filter((s) => s && !s.startsWith("#"))
              : []
            if (!list.length) return send(400, { ok: false, error: "phrases.txt 是空的" })
            const engine = url.searchParams.get("engine") || await resolveVoiceEngine()
            warmPhrases(list, engine)
            return send(200, { ok: true, started: true, total: list.length, engine })
          } catch (error) {
            return send(500, { ok: false, error: String(error) })
          }
        }
        if (url.pathname === "/lanyuxi/voice") {
          const home = config.homeDir || "E:\\dsh-foundry\\lanyuxi-home"
          const engineDir = config.gptSovitsDir || "E:\\GPT-SoVITS"
          const countFiles = (dir) => {
            try { return existsSync(dir) ? readdirSync(dir).length : 0 } catch { return 0 }
          }
          let ffmpegOk = false
          try { ffmpegOk = spawnSync(config.ffmpegPath || "ffmpeg", ["-version"], { windowsHide: true, timeout: 5000 }).status === 0 } catch {}
          const active = await resolveVoiceEngine()
          const alive = await engineAlive()
          if (alive && voiceState.status !== "running") { voiceState.status = "running" }
          return send(200, {
            activeEngine: active,
            selectedEngine: settings.voiceEngine,
            voiceLang: settings.voiceLang,
            refText: (() => { try { const p = join(home, "voice", "ref.txt"); return existsSync(p) ? readFileSync(p, "utf8").split(/\r?\n/).map((s) => s.trim()).filter((s) => s && !s.startsWith("#")).join("") : (config.gptSovitsPrompt || "") } catch { return "" } })(),
            installDir: voiceSettingsDir(),
            installOk: engineLaunchTarget().ok,
            process: voiceState,
            engineAlive: alive,
            warm: warmState,
            engine: { dir: engineDir, installed: existsSync(engineDir) },
            ffmpeg: ffmpegOk,
            samples: countFiles(join(home, "voice")),
            cache: countFiles(join(home, "voice", "cache")),
            enabled: Boolean(config.voiceEnabled),
            // 两档情绪还没落地（只有一份 ref.wav），面板照实说
            emotionTiers: [],
            emotionImplemented: false,
          })
        }
        if (url.pathname === "/lanyuxi/assets") {
          const home = config.homeDir || "E:\\dsh-foundry\\lanyuxi-home"
          const listDir = (dir) => {
            try {
              if (!existsSync(dir)) return []
              return readdirSync(dir, { withFileTypes: true })
                .filter((e) => e.isFile())
                .map((e) => {
                  const st = statSync(join(dir, e.name))
                  return { name: e.name, size: st.size, mtime: st.mtimeMs }
                })
                .sort((a, b) => b.mtime - a.mtime)
            } catch { return [] }
          }
          const countMood = (mood) => listDir(join(home, "stickers", mood)).length
          const monthDir = (base) => join(home, base, new Date().getFullYear() + "-" + z2(new Date().getMonth() + 1))
          const diaryList = (kind) => {
            const out = []
            try {
              const root = join(home, "diary", kind)
              if (!existsSync(root)) return out
              for (const month of readdirSync(root, { withFileTypes: true })) {
                if (!month.isDirectory()) continue
                for (const file of listDir(join(root, month.name))) out.push({ name: month.name + "/" + file.name, mtime: file.mtime })
              }
            } catch {}
            return out.sort((a, b) => b.mtime - a.mtime).slice(0, 8)
          }
          return send(200, {
            stickers: { happy: countMood("happy"), speechless: countMood("speechless"), coy: countMood("coy"), fierce: countMood("fierce") },
            recentStickers: [].concat(
              listDir(join(home, "stickers", "happy")).slice(0, 2).map((x) => ({ name: "开心/" + x.name, size: x.size })),
              listDir(join(home, "stickers", "speechless")).slice(0, 1).map((x) => ({ name: "无语/" + x.name, size: x.size })),
              listDir(join(home, "stickers", "coy")).slice(0, 1).map((x) => ({ name: "撒娇/" + x.name, size: x.size })),
              listDir(join(home, "stickers", "fierce")).slice(0, 1).map((x) => ({ name: "凶/" + x.name, size: x.size })),
            ),
            images: listDir(monthDir("images")).slice(0, 6).map((x) => ({ name: x.name, size: x.size, mtime: x.mtime })),
            diary: { toOwner: diaryList("to-owner"), private: diaryList("private") },
          })
        }
        if (url.pathname === "/lanyuxi/growth") {
          const home = config.homeDir || "E:\\dsh-foundry\\lanyuxi-home"
          // 只读聚合：文件不在就是空数据，readGrowth 自己兜住异常，这里不会 500
          return send(200, readGrowth({ homeDir: home, limit: url.searchParams.get("limit"), kind: url.searchParams.get("kind") }))
        }
        if (url.pathname === "/lanyuxi/overview") {
          const countFiles = (dir) => {
            try {
              if (!existsSync(dir)) return 0
              let n = 0
              const stack = [dir]
              while (stack.length) {
                const cur = stack.pop()
                for (const entry of readdirSync(cur, { withFileTypes: true })) {
                  if (entry.isDirectory()) stack.push(join(cur, entry.name))
                  else n += 1
                }
              }
              return n
            } catch { return 0 }
          }
          const home = config.homeDir || "E:\\dsh-foundry\\lanyuxi-home"
          const recent = []
          let sentToday = 0
          let receivedToday = 0
          try {
            const dir = join(config.sessionCwd || process.cwd(), "transcript", new Date().getFullYear() + "-" + z2(new Date().getMonth() + 1))
            const file = join(dir, z2(new Date().getDate()) + ".md")
            if (existsSync(file)) {
              const lines = readFileSync(file, "utf8").split(/\r?\n/).filter(Boolean)
              for (const line of lines) {
                if (line.indexOf("**蓝余汐**") >= 0) sentToday += 1
                else receivedToday += 1
              }
              for (const line of lines.slice(-10).reverse()) recent.push(line)
            }
          } catch {}
          const cur = sessionState.current
          return send(200, {
            connected, verified, selfId, selfNickname, ownerQq: config.ownerQq,
            mood: moodLine(),
            allowGroups: config.allowGroups || [],
            muted: muted(), socialPaused: Boolean(brakes.socialPaused), biliPaused: Boolean(brakes.biliPaused),
            todaySent: sentToday || quota.count,
            sentToday,
            receivedToday,
            maxOutboundPerDay: config.maxOutboundPerDay,
            received: diag.messages,
            blocked: diag.guardBlocked || 0,
            mutedSkip: diag.mutedSkip || 0,
            socialSkip: diag.socialSkip || 0,
            session: cur ? { id: cur.id, segment: sessionState.seq || 1, chars: cur.chars, limit: currentLimit(), percent: Math.round(cur.chars / currentLimit() * 100) } : null,
            recent,
            assets: {
              stickers: countFiles(join(home, "stickers")),
              images: countFiles(join(home, "images")),
              diary: countFiles(join(home, "diary")),
              backups: existsSync(join(config.sessionCwd || process.cwd(), "backup")) ? readdirSync(join(config.sessionCwd || process.cwd(), "backup")).length : 0,
            },
          })
        }
        if (url.pathname === "/lanyuxi/brakes") {
          const snapshot = () => ({
            muted: muted(),
            muteUntil: Number(brakes.muteUntil || 0),
            socialPaused: Boolean(brakes.socialPaused),
            biliPaused: Boolean(brakes.biliPaused),
            guardBlocked: diag.guardBlocked || 0,
            mutedSkip: diag.mutedSkip || 0,
            socialSkip: diag.socialSkip || 0,
          })
          if (req.method === "POST") {
            const action = url.searchParams.get("action") || ""
            if (action === "mute") brakes.muteUntil = Date.now() + 2 * 60 * 60 * 1000
            if (action === "unmute") brakes.muteUntil = 0
            if (action === "social-on") brakes.socialPaused = true
            if (action === "social-off") brakes.socialPaused = false
            if (action === "bili-on") brakes.biliPaused = true
            if (action === "bili-off") brakes.biliPaused = false
            saveBrakes()
            return send(200, snapshot())
          }
          return send(200, snapshot())
        }
        if (url.pathname === "/lanyuxi/persona") {
          if (req.method === "POST") {
            let body = ""
            req.on("data", (chunk) => { body += chunk })
            req.on("end", () => {
              try {
                const payload = body ? JSON.parse(body) : {}
                if (typeof payload.card === "string") writePersonaFile("人格卡.md", payload.card)
                if (typeof payload.aliases === "string") writePersonaFile("称呼.txt", payload.aliases)
                if (typeof payload.addFix === "string") addFix(payload.addFix)
                if (typeof payload.banned === "string") writePersonaFile("禁说清单.md", payload.banned)
                send(200, { ok: true, card: readPersonaFile("人格卡.md"), fixes: fixList(), aliases: ownerAliases(), banned: guardTerms() })
              } catch (error) {
                send(500, { ok: false, error: String(error) })
              }
            })
            return
          }
          return send(200, { card: readPersonaFile("人格卡.md"), fixes: fixList(), aliases: ownerAliases(), banned: guardTerms() })
        }
        if (url.pathname === "/lanyuxi/open") {
          const target = url.searchParams.get("target") || ""
          const home = config.homeDir || "E:\\dsh-foundry\\lanyuxi-home"
          const map = {
            backup: join(config.sessionCwd || process.cwd(), "backup"),
            transcript: join(config.sessionCwd || process.cwd(), "transcript"),
            home,
            persona: join(home, "persona"),
            diary: join(home, "diary"),
            stickers: join(home, "stickers"),
            images: join(home, "images"),
          }
          const dir = map[target]
          if (!dir) return send(400, { ok: false, error: "不认识的目录" })
          try {
            mkdirSync(dir, { recursive: true })
            spawn("explorer.exe", [dir], { detached: true, stdio: "ignore", windowsHide: false }).unref()
            return send(200, { ok: true, dir })
          } catch (error) {
            return send(500, { ok: false, error: String(error), dir })
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
            const en = url.searchParams.get("backupEnabled")
            if (en === "1" || en === "0") {
              settings.backupEnabled = en === "1"
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
            backupEnabled: settings.backupEnabled,
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
      selfNickname = String(info?.nickname ?? selfNickname)
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
          // 不再回 token 字段：面板只用 url，别的地方也不需要看见它
          return send(200, { url })
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
      if (!settings.backupEnabled) return
      const interval = (settings.backupHours || 24) * 60 * 60 * 1000
      if (lastBackupAt && Date.now() - lastBackupAt < interval) return
      const idle = !lastActivityAt || Date.now() - lastActivityAt > 12 * 60 * 60 * 1000
      if (idle) return
      if (!sessionsChangedSince(lastBackupAt)) return
      backupSessions("scheduled")
    }, 60 * 1000)
    return () => clearInterval(backupTimer)
  }, "lanyuxi: 会话备份")

  ctx.effect(() => {
    const timer = setTimeout(() => {
      const want = settings.voiceEngine || config.voiceEngine || "auto"
      if (want === "off" || want === "sapi" || want === "edge-tts") return
      startEngine().catch((error) => { diag.voiceError = String(error) })
    }, 5000)
    return () => clearTimeout(timer)
  }, "lanyuxi: 语音引擎自启")

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