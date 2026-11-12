/**
 * 蓝余汐 · 记忆 v0（写入层 + 只读聚合）
 *
 * 只管一件事：往 lanyuxi-home\memory\ 下的 JSONL 追加事件，再把四个日志读出来给面板。
 * 字段以 docs\记忆v0-数据契约.md 为准，改这里之前先改那份文档。
 *
 * 规矩：
 * 1. 只追加，不改写；一行一个 JSON，缺的字段就不写那个字段（不写 null）。
 * 2. 写：logEvent 会抛错，调用方必须 try/catch（写不进去不许影响聊天）。
 * 3. 读：readGrowth 绝不抛错、绝不写文件，文件不存在就是空数据。
 */

import { readFileSync, existsSync, statSync, mkdirSync, appendFileSync } from "node:fs"
import { join, basename } from "node:path"

const DEFAULT_HOME = "E:\\dsh-foundry\\lanyuxi-home"
const TRAITS = ["initiative", "curiosity", "humor", "clinging", "tsundere"]
const PERSONA_CARD = "人格卡.md"

/** 进程内递增的事件序号 */
let seq = 0

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
  seq += 1
  const row = {
    schemaVersion: 1,
    id: "evt_" + ymd(now) + "_" + String(seq).padStart(6, "0"),
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
