/**
 * 蓝余汐 · 记忆 v0.5（写入层 + 只读聚合 + 检索）
 *
 * 只管一件事：往 lanyuxi-home\memory\ 下的 JSONL 追加记录，再把日志读出来给面板和提示用。
 * 字段以 docs\记忆v0-数据契约.md 为准，改这里之前先改那份文档。
 *
 * 规矩：
 * 1. 只追加，不改写；一行一个 JSON，缺的字段就不写那个字段（不写 null）。
 * 2. 写：logEvent / saveMemory / savePersonalitySample 都会抛错，调用方必须 try/catch（写不进去不许影响聊天）。
 * 3. 读：readGrowth / recall 绝不抛错、绝不写文件，文件不存在就是空数据。
 */

import { readFileSync, existsSync, statSync, mkdirSync, appendFileSync } from "node:fs"
import { join, basename } from "node:path"

const DEFAULT_HOME = "E:\\dsh-foundry\\lanyuxi-home"
const TRAITS = ["initiative", "curiosity", "humor", "clinging", "tsundere"]
const PERSONA_CARD = "人格卡.md"

/**
 * 各文件的下一个号（文件绝对路径 → 下一个号）。第一次写某个文件之前先「续号」：
 * 读它末尾能解析的那条记录，取 id 的 6 位数字后缀 + 1；取不到就从 1 开始。
 * 续过一次就记在这里，同一进程内不再扫文件。
 * 不续号的话，进程重启（DSH 重载、崩溃重连）会写出重复 id，memories 的 sourceEvent 就指错了。
 */
const seqNext = new Map()

/** 取下一个号并占住它；同一个文件只续一次号 */
function takeSeq(file) {
  const cached = seqNext.get(file)
  if (cached !== undefined) {
    seqNext.set(file, cached + 1)
    return cached
  }
  const rows = readJsonl(file).rows // 整读、坏行跳过（和 readGrowth 共用同一个读法）
  const last = rows.length ? rows[rows.length - 1] : null
  const suffix = /_(\d{6})$/.exec(String(last?.id ?? ""))
  const next = suffix ? Number(suffix[1]) + 1 : 1
  seqNext.set(file, next + 1)
  return next
}

const p2 = (n) => String(n).padStart(2, "0")
const ymd = (d) => "" + d.getFullYear() + p2(d.getMonth() + 1) + p2(d.getDate())

/** 本地时间的 ISO（自己拼，带时区偏移；toISOString 是 UTC，不能用） */
function localIso(date = new Date()) {
  const off = -date.getTimezoneOffset()
  const abs = Math.abs(off)
  return date.getFullYear() + "-" + p2(date.getMonth() + 1) + "-" + p2(date.getDate()) +
    "T" + p2(date.getHours()) + ":" + p2(date.getMinutes()) + ":" + p2(date.getSeconds()) +
    (off >= 0 ? "+" : "-") + p2(Math.floor(abs / 60)) + ":" + p2(abs % 60)
}

/** 截断到最多 max 字，超了补省略号（加完省略号也不许超） */
function clamp(text, max) {
  const s = String(text ?? "").trim()
  return s.length > max ? s.slice(0, max - 1) + "…" : s
}

/** QQ 消息正文 → 一句话摘要：剥 CQ 码、把换行压成空格、最多 120 字 */
export function messageSummary(text, max = 120) {
  const plain = String(text ?? "").replace(/\[CQ:[^\]]*\]/g, "").replace(/\s+/g, " ").trim()
  return clamp(plain, max)
}

function memoryDir(homeDir) {
  return join(String(homeDir || DEFAULT_HOME), "memory")
}

/** 路径在不在（还得是目录；被同名的文件占着也算不在） */
function dirExists(path) {
  try { return existsSync(path) && statSync(path).isDirectory() } catch { return false }
}

/**
 * 往 events.jsonl 追加一条事件，返回写进去的那一行。
 * 会抛错：调用方接住它，记进 diag.memoryError 就算完事。
 */
export function logEvent(homeDir, event) {
  const dir = memoryDir(homeDir)
  mkdirSync(dir, { recursive: true })
  const now = new Date()
  const n = takeSeq(join(dir, "events.jsonl"))
  const row = {
    schemaVersion: 1,
    id: "evt_" + ymd(now) + "_" + String(n).padStart(6, "0"),
    at: localIso(now),
    kind: String(event?.kind || "action"),
    platform: String(event?.platform || "local"),
    who: String(event?.who || "她"),
    summary: clamp(event?.summary, 120),
    perms: String(event?.perms || "system"),
  }
  const ref = String(event?.ref ?? "").trim()
  if (ref) row.ref = ref
  appendFileSync(join(dir, "events.jsonl"), JSON.stringify(row) + "\n", "utf8")
  return row
}

/** 一行一行读 JSONL：读不了的（坏行）跳过并计数，文件不存在就当空的 */
function readJsonl(file) {
  const out = { rows: [], skipped: 0 }
  try {
    if (!existsSync(file) || !statSync(file).isFile()) return out
    for (const line of readFileSync(file, "utf8").split(/\r?\n/)) {
      const raw = line.trim()
      if (!raw) continue
      try {
        const row = JSON.parse(raw)
        if (row && typeof row === "object" && !Array.isArray(row)) out.rows.push(row)
        else out.skipped += 1
      } catch {
        out.skipped += 1
      }
    }
  } catch {
    out.skipped += 1
  }
  return out
}

function clampLimit(limit) {
  const n = Number(limit)
  if (!Number.isFinite(n) || n <= 0) return 50
  return Math.min(Math.floor(n), 200)
}

/** 时间倒序；同一秒里再按 id 倒序（id 进程内递增，后写的排前面） */
const newestFirst = (a, b) => {
  const diff = String(b?.at ?? "").localeCompare(String(a?.at ?? ""))
  return diff !== 0 ? diff : String(b?.id ?? "").localeCompare(String(a?.id ?? ""))
}

/** 五个维度摊平成固定形状：有值给数字，没值给 null（前端好画） */
function traitsOf(record) {
  const traits = {}
  for (const key of TRAITS) {
    const value = record && record.traits ? Number(record.traits[key]) : NaN
    traits[key] = Number.isFinite(value) ? value : null
  }
  return traits
}

/** 人格卡文件的小档案：路径、在不在、多大、开头 300 字 */
function cardOf(home) {
  const card = { path: basename(String(home)) + "/persona/" + PERSONA_CARD, exists: false, bytes: 0, head: "" }
  try {
    const text = readFileSync(join(String(home), "persona", PERSONA_CARD), "utf8")
    return { path: card.path, exists: true, bytes: Buffer.byteLength(text, "utf8"), head: text.slice(0, 300) }
  } catch {
    return card
  }
}

function emptyGrowth(home, error) {
  const dir = memoryDir(home)
  const growth = {
    ok: true,
    generatedAt: localIso(),
    counts: { events: 0, memories: 0, skills: 0, personalitySamples: 0 },
    timeline: [],
    personality: { latest: null, latestAt: null, traits: traitsOf(null), samples: 0, history: [], card: cardOf(home) },
    files: { dir, exists: dirExists(dir) },
    skipped: 0,
  }
  if (error) growth.error = String(error)
  return growth
}

function buildGrowth(home, options) {
  const dir = memoryDir(home)
  const cap = clampLimit(options?.limit)
  const kind = String(options?.kind ?? "").trim()
  const events = readJsonl(join(dir, "events.jsonl"))
  const memories = readJsonl(join(dir, "memories.jsonl"))
  const skills = readJsonl(join(dir, "skills.jsonl"))
  const personality = readJsonl(join(dir, "personality.jsonl"))

  // 时间线：先按 kind 过滤，再按时间倒序，最后才截 limit
  const timeline = events.rows
    .filter((row) => !kind || String(row?.kind ?? "") === kind)
    .slice()
    .sort(newestFirst)
    .slice(0, cap)
    .map((row) => {
      const item = {
        at: String(row?.at ?? ""),
        kind: String(row?.kind ?? ""),
        platform: String(row?.platform ?? ""),
        who: String(row?.who ?? ""),
        summary: clamp(row?.summary, 120),
        perms: String(row?.perms ?? ""),
      }
      if (row?.ref) item.ref = String(row.ref)
      return item
    })

  const ordered = personality.rows.slice().sort(newestFirst)
  const last = ordered.length ? ordered[0] : null
  let latest = null
  if (last) {
    latest = { id: String(last.id ?? ""), at: String(last.at ?? ""), kind: String(last.kind ?? ""), traits: traitsOf(last) }
    if (last.why) latest.why = String(last.why)
  }
  // 历史只记正式变更（sample 是采样，不进历史）
  const history = ordered
    .filter((row) => String(row?.kind ?? "") === "change")
    .sort(newestFirst)
    .map((row) => {
      const item = { at: String(row?.at ?? ""), kind: "change" }
      if (row?.why) item.why = String(row.why)
      return item
    })

  return {
    ok: true,
    generatedAt: localIso(),
    counts: {
      events: events.rows.length,
      memories: memories.rows.length,
      skills: skills.rows.length,
      personalitySamples: personality.rows.filter((row) => String(row?.kind ?? "") === "sample").length,
    },
    timeline,
    personality: {
      latest,
      latestAt: last ? String(last.at ?? "") : null,
      traits: traitsOf(last),
      samples: personality.rows.filter((row) => String(row?.kind ?? "") === "sample").length,
      history,
      card: cardOf(home),
    },
    files: { dir, exists: dirExists(dir) },
    skipped: events.skipped + memories.skipped + skills.skipped + personality.skipped,
  }
}

/**
 * 面板要的「成长」数据。只读、不写任何文件、绝不抛错。
 * 日志还没建 = 空数据（HTTP 200），不是错误。
 */
export function readGrowth(options = {}) {
  const home = String(options?.homeDir || DEFAULT_HOME)
  try {
    return buildGrowth(home, options)
  } catch (error) {
    return emptyGrowth(home, error)
  }
}

// —— v0.5：记忆写进来、人格采样写进来、提示能捞出去 ——

const MEMORY_TYPES = ["episodic", "person", "knowledge", "skill", "self"]
const VISIBILITIES = ["private", "shared", "public"]

/**
 * 往 memories.jsonl 追加一条记忆，返回写进去的那一行。
 * 会抛错：调用方接住它，记进 diag.memoryError 就算完事。
 */
export function saveMemory(homeDir, memory) {
  const content = clamp(memory?.content, 200)
  if (!content) throw new Error("记忆内容是空的")
  const type = String(memory?.type || "episodic")
  if (!MEMORY_TYPES.includes(type)) throw new Error("记忆类型只能是 " + MEMORY_TYPES.join("/"))
  const visibility = String(memory?.visibility || "private")
  if (!VISIBILITIES.includes(visibility)) throw new Error("可见性只能是 " + VISIBILITIES.join("/"))
  const dir = memoryDir(homeDir)
  mkdirSync(dir, { recursive: true })
  const now = new Date()
  const n = takeSeq(join(dir, "memories.jsonl"))
  const row = {
    schemaVersion: 1,
    id: "mem_" + ymd(now) + "_" + String(n).padStart(6, "0"),
    at: localIso(now),
    type,
    content,
    visibility,
  }
  const sourceEvent = String(memory?.sourceEvent ?? "").trim()
  if (sourceEvent) row.sourceEvent = sourceEvent
  const confidence = Number(memory?.confidence)
  if (Number.isFinite(confidence)) row.confidence = Math.min(1, Math.max(0, confidence))
  appendFileSync(join(dir, "memories.jsonl"), JSON.stringify(row) + "\n", "utf8")
  return row
}

/**
 * 往 personality.jsonl 追加一条本周样本，返回写进去的那一行。
 * 只写给了的维度，缺的不写键（不写 null）。会抛错。
 */
export function savePersonalitySample(homeDir, sample) {
  const traits = {}
  for (const key of TRAITS) {
    const raw = sample?.traits ? sample.traits[key] : undefined
    if (raw === undefined || raw === null || raw === "") continue
    const value = Number(raw)
    if (!Number.isFinite(value) || value < 0 || value > 1) throw new Error("traits." + key + " 要给 0 到 1 之间的数")
    traits[key] = value
  }
  if (!Object.keys(traits).length) throw new Error("traits 至少要有一个维度")
  const dir = memoryDir(homeDir)
  mkdirSync(dir, { recursive: true })
  const now = new Date()
  const n = takeSeq(join(dir, "personality.jsonl"))
  const row = {
    schemaVersion: 1,
    id: "per_" + ymd(now) + "_" + String(n).padStart(6, "0"),
    at: localIso(now),
    kind: "sample",
    traits,
  }
  const why = clamp(sample?.why, 120)
  if (why) row.why = why
  appendFileSync(join(dir, "personality.jsonl"), JSON.stringify(row) + "\n", "utf8")
  return row
}

/**
 * 检索词：按非汉字/非字母数字切开再去重。
 * 中文没空格，一整句会被切成一个词、等于永远匹配不到，所以汉语长词再补上它的两字词（短、权重低）。
 */
function wordsOf(text) {
  const out = new Set()
  for (const word of String(text ?? "").toLowerCase().split(/[^\u4e00-\u9fa5a-z0-9]+/)) {
    if (!word) continue
    out.add(word)
    if (word.length > 3 && /^[\u4e00-\u9fa5]+$/.test(word)) {
      for (let i = 0; i + 2 <= word.length; i += 1) out.add(word.slice(i, i + 2))
    }
  }
  return [...out].slice(0, 80) // 词太多就只用前面的，一次检索不值得把自己拖慢
}

/**
 * 捞几条跟这句话有关的记忆。只读、绝不抛错、绝不写文件。
 * 群聊只给 shared/public —— private 的事不许带到群里。
 */
export function recall(homeDir, options = {}) {
  const empty = []
  try {
    const file = join(memoryDir(homeDir), "memories.jsonl")
    if (!existsSync(file) || !statSync(file).isFile()) return empty
    const group = String(options?.platform ?? "") === "group"
    const words = wordsOf(options?.text)
    if (!words.length) return empty
    let lines = readFileSync(file, "utf8").split(/\r?\n/)
    // 性能护栏：账本超过 5000 行就只读最后 2000 行，检索这种活不值得为老记忆翻全本
    if (lines.length > 5000) lines = lines.slice(-2000)

    const hits = []
    for (const line of lines) {
      const raw = line.trim()
      if (!raw) continue
      let row
      try { row = JSON.parse(raw) } catch { continue } // 坏行跳过
      if (!row || typeof row !== "object") continue
      const visibility = String(row.visibility || "private")
      if (group && visibility !== "shared" && visibility !== "public") continue
      const content = String(row.content ?? "").trim()
      if (!content) continue
      const body = content.toLowerCase()
      let score = 0
      for (const word of words) if (body.includes(word)) score += word.length * word.length // 词越长压得越狠
      if (score <= 0) continue
      hits.push({ score, row, content, visibility })
    }

    // 分高的在前；同分时新的在前
    hits.sort((a, b) => b.score - a.score || newestFirst(a.row, b.row))
    const asked = Number(options?.limit)
    const limit = Number.isFinite(asked) && asked > 0 ? Math.min(Math.floor(asked), 10) : 5
    return hits.slice(0, limit).map((hit) => ({
      id: String(hit.row.id ?? ""),
      at: String(hit.row.at ?? ""),
      type: String(hit.row.type ?? ""),
      content: hit.content.slice(0, 200),
      visibility: hit.visibility,
    }))
  } catch {
    return empty
  }
}
