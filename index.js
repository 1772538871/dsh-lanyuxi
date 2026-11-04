/**
 * 蓝余汐 · QQ 通道（M1）
 *
 * 设计原则：零配置。所有连接参数（WS 端口 / accessToken / QQ 号）都从 NapCat 自己的
 * 配置里读，不再让主人填一遍。她在 DSH 里只有一个会话，所有聊天都汇进这一个大脑。
 */

import { readFileSync, existsSync, mkdirSync, writeFileSync, statSync } from "node:fs"
import { join } from "node:path"
import { randomUUID } from "node:crypto"
import { spawn } from "node:child_process"

export const name = "lanyuxi"
export const inject = ["tools"]

const DEFAULTS = {
  napcatDir: "E:\\NapCat",
  ownerQq: "",
  allowGroups: [],
  freeGroups: [],
  sessionCwd: "",
  cooldownMs: 300000,
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
  /** 返回 undefined 表示可以发，返回字符串表示为什么不行。 */
  check(chatKey) {
    this.roll()
    if (this.count >= this.config.maxOutboundPerDay) return `今天已经说了 ${this.count} 句，到上限了`
    const last = this.lastByChat.get(chatKey) ?? 0
    const wait = this.config.cooldownMs - (Date.now() - last)
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
    if (event.user_id !== undefined && String(event.user_id) === selfId) return

    const messageType = event.message_type
    const groupId = event.group_id === undefined ? "" : String(event.group_id)
    const userId = String(event.user_id ?? "")
    const segments = Array.isArray(event.message) ? event.message : []
    const atMe = segments.some((s) => s?.type === "at" && String(s?.data?.qq) === selfId)
    const text = segments.filter((s) => s?.type === "text").map((s) => s?.data?.text ?? "").join("").trim()
      || (typeof event.raw_message === "string" ? event.raw_message.trim() : "")
    if (!text) return

    if (messageType === "group") {
      if (!config.allowGroups.includes(groupId)) return
      const free = config.freeGroups.includes(groupId)
      if (!free && !atMe) return
    } else if (messageType === "private") {
      if (config.ownerQq && userId !== config.ownerQq) return
    } else {
      return
    }

    if (remember(event.message_id)) return

    const chatKey = chatKeyOf(messageType, groupId, userId)
    lastChat = { messageType, groupId, userId, chatKey }

    const who = messageType === "group"
      ? `群 ${groupId} 里的 ${event.sender?.card || event.sender?.nickname || userId}`
      : "主人"
    const prompt = [
      `【QQ · ${messageType === "group" ? "群聊" : "私聊"}】${who} 说：${text}`,
      `要回话就调用 qq_send 工具（不填 target 默认回给当前这个聊天）。不想回就别调用，不调用等于不说话。`,
    ].join("\n")

    await deliver(prompt)
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

  /** 把消息灌进她那个会话，让她醒过来。 */
  async function deliver(text) {
    const controller = ctx.get("sessionController")
    if (!controller) throw new Error("sessionController 不可用")
    const id = await ensureSession()
    await controller.prompt({
      requestId: randomUUID(),
      sessionId: id,
      mode: "queue",
      content: [{ type: "text", text }],
    })
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
      }).catch(() => {})
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
      handleEvent(payload).catch((error) => logger.warn?.("[lanyuxi] 处理消息失败: " + String(error)))
    }
    ws.onclose = () => {
      connected = false
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
      const blocked = quota.check(chatKey)
      if (blocked) throw new Error(blocked + "（这是守门员拦下的，不是故障）")

      const action = type === "group" ? "send_group_msg" : "send_private_msg"
      const params = type === "group" ? { group_id: Number(id), message: [{ type: "text", data: { text: args.text } }] } : { user_id: Number(id), message: [{ type: "text", data: { text: args.text } }] }
      await request(action, params)
      quota.mark(chatKey)
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
        qrFileSeen: existsSync(join(config.napcatDir, "napcat", "cache", "qrcode.png")),
      }, null, 2)
    },
  })

  // —— 面板用的三个 HTTP 接口：起 NapCat / 取二维码 / 查状态 ——
  const qrPath = join(config.napcatDir, "napcat", "cache", "qrcode.png")
  let napcatStarting = false

  function startNapcat() {
    if (napcatStarting) return { ok: true, already: true }
    const bat = join(config.napcatDir, "napcat.bat")
    if (!existsSync(bat)) return { ok: false, error: "找不到 " + bat }
    napcatStarting = true
    try {
      const child = spawn("cmd.exe", ["/c", bat], {
        cwd: config.napcatDir,
        detached: true,
        stdio: "ignore",
        windowsHide: false,
      })
      child.unref()
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
      const script = "Get-Process | Where-Object { $_.Path -like '" + pattern + "*' -or $_.ProcessName -eq 'NapCatWinBootMain' } | Stop-Process -Force -ErrorAction SilentlyContinue"
      spawn("powershell", ["-NoProfile", "-Command", script], { detached: true, stdio: "ignore", windowsHide: true }).unref()
      logger.info?.("[lanyuxi] 已请求关闭 NapCat")
      return { ok: true }
    } catch (error) {
      logger.warn?.("[lanyuxi] 关闭 NapCat 失败: " + String(error))
      return { ok: false, error: String(error) }
    }
  }

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
          return send(200, { connected, selfId, allowGroups: config.allowGroups, panel: config.napcatPanel })
        }
        if (url.pathname === "/lanyuxi/start") {
          const result = startNapcat()
          return send(result.ok ? 200 : 500, result)
        }
        if (url.pathname === "/lanyuxi/stop") {
          const result = stopNapcat()
          return send(result.ok ? 200 : 500, result)
        }
        if (url.pathname === "/lanyuxi/qr") {
          try {
            if (!existsSync(qrPath)) return send(404, { error: "还没有二维码（NapCat 可能没在跑）" })
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
