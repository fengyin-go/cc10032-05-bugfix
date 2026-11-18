/** 迟到处理 / 补数任务的接口封装：阈值口径、区段覆盖与导入导出都在这里对齐后端。 */
import { request } from '@/api/client'

export type ThresholdInfo = {
  policy: string
  threshold_percent: number
  threshold_ratio: number
  threshold_source: string
  threshold_fallback: boolean
  allowed_lateness: number
}

export type LateBatch = {
  批次号: string
  文件名: string
  文件类型: string
  导入时间: number
  总事件数: number
  迟到数: number
  迟到比例: number
  阈值百分比: number
  阈值来源: string
  允许延迟: number
  水位: number
  触发: boolean
  区段数: number
  作业: number[]
  幂等重复: boolean
}

export type LateSegmentJob = {
  job_id: number
  job_name: string
  status: string
  progress: string
  paused: boolean
  executable: boolean
}

export type BackfillSegment = {
  segment_id: string
  policy: string
  source: string
  start: number
  end: number
  range: string
  late_count: number
  version: number
  jobs: LateSegmentJob[]
  covered: boolean
  job_id: number | null
}

export type LateJob = {
  job_id: number
  job_name: string
  status: string
  range: string
  progress: string
  paused: boolean
  executable: boolean
  segments: string[]
  progress_log: { 批次?: string; 时间: number; 新增事件?: number; 范围?: string; 说明: string }[]
}

export type Coverage = {
  segment_count: number
  covered: number
  uncovered: string[]
  complete: boolean
}

export type LateProcessingDetail = {
  ok: boolean
  policy: string
  settings: ThresholdInfo
  latest_batch: Partial<LateBatch>
  actual_late_ratio: number
  actual_threshold_ratio: number
  actual_threshold_percent: number
  batches: LateBatch[]
  segments: (BackfillSegment & { 区段ID?: string; 迟到事件数?: number; 版本?: number })[]
  jobs: LateJob[]
  coverage: Coverage
  bypass_count: number
}

export type BackfillConsole = {
  ok: boolean
  policies: Record<
    string,
    {
      policy: string
      segments: BackfillSegment[]
      jobs: LateJob[]
      coverage: Coverage
    }
  >
  pending_jobs: {
    job_id: number
    job_name: string
    range: string
    status: string
    progress: string
    paused: boolean
    executable: boolean
    segments: string[]
  }[]
  totals: { segments: number; uncovered: number }
}

export type VerifyResult = {
  ok: boolean
  valid: boolean
  issues: string[]
  coverage?: Coverage
  summary?: Record<string, number | boolean>
  duplicate?: boolean
  already_imported?: boolean
  backfill?: {
    segment_count: number
    job_count: number
    missing_jobs: string[]
    duplicate_ranges: string[]
  }
}

async function readJson<T>(response: Response, fallback: string): Promise<T> {
  const payload = (await response.json().catch(() => null)) as (T & { message?: string }) | null
  if (!response.ok) {
    throw new Error(payload?.message ?? `${fallback}（HTTP ${response.status}）`)
  }
  return payload as T
}

function encodePolicy(policy: string): string {
  return encodeURIComponent(policy)
}

export async function fetchLateProcessing(policy: string): Promise<LateProcessingDetail> {
  return readJson<LateProcessingDetail>(
    await request(`/api/late_handler/${encodePolicy(policy)}/processing`),
    '迟到处理详情读取失败',
  )
}

export async function ingestLateFile(
  policy: string,
  content: string,
  filename: string,
  pausedStrategy: 'extend' | 'split',
): Promise<{ ok: boolean; message: string; duplicate?: boolean; committed?: boolean }> {
  return readJson(
    await request(`/api/late_handler/${encodePolicy(policy)}/ingest`, {
      method: 'POST',
      body: JSON.stringify({ content, filename, paused_strategy: pausedStrategy }),
    }),
    '迟到事件导入失败',
  )
}

export async function importSameSourceFile(
  policy: string,
  content: string,
  filename: string,
  pausedStrategy: 'extend' | 'split',
): Promise<{ ok: boolean; message: string; duplicate?: boolean }> {
  return readJson(
    await request(`/api/late_handler/${encodePolicy(policy)}/import`, {
      method: 'POST',
      body: JSON.stringify({ content, filename, paused_strategy: pausedStrategy }),
    }),
    '同源导出文件接收失败',
  )
}

export async function verifyLateFile(policy: string, content: string): Promise<VerifyResult> {
  return readJson<VerifyResult>(
    await request(`/api/late_handler/${encodePolicy(policy)}/verify`, {
      method: 'POST',
      body: JSON.stringify({ content }),
    }),
    '导出文件校验失败',
  )
}

export async function fetchBackfillConsole(): Promise<BackfillConsole> {
  return readJson<BackfillConsole>(await request('/api/backfill_job/console'), '补数任务读取失败')
}

export async function verifyBackfillManifest(content: string): Promise<VerifyResult> {
  return readJson<VerifyResult>(
    await request('/api/backfill_job/verify', {
      method: 'POST',
      body: JSON.stringify({ content }),
    }),
    '补数任务清单校验失败',
  )
}

export async function splitPausedJob(jobId: number): Promise<{ ok: boolean; message: string }> {
  return readJson(
    await request(`/api/backfill_job/${jobId}/split`, { method: 'POST', body: '{}' }),
    '分段作业失败',
  )
}

export async function downloadFile(path: string, filename: string): Promise<void> {
  const response = await request(path)
  if (!response.ok) throw new Error(`文件下载失败（HTTP ${response.status}）`)
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = filename
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export function lateManifestPath(policy: string): string {
  return `/api/late_handler/${encodePolicy(policy)}/export`
}

export function lateDetailCsvPath(policy: string): string {
  return `/api/late_handler/${encodePolicy(policy)}/export-detail.csv`
}

export function lateBatchesCsvPath(policy: string): string {
  return `/api/late_handler/${encodePolicy(policy)}/batches.csv`
}

export function backfillManifestPath(policy: string | null): string {
  const query = policy ? `?policy=${encodeURIComponent(policy)}` : ''
  return `/api/backfill_job/manifest${query}`
}

export function backfillTasksCsvPath(policy: string | null): string {
  const query = policy ? `?policy=${encodeURIComponent(policy)}` : ''
  return `/api/backfill_job/tasks.csv${query}`
}

/** 浏览器里读文件：统一按 UTF-8 文本读取，BOM 交给后端处理。 */
export function readFileText(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => resolve(String(reader.result ?? ''))
    reader.onerror = () => reject(new Error(`文件「${file.name}」读取失败`))
    reader.readAsText(file, 'utf-8')
  })
}

export function formatPercent(ratio: number): string {
  return `${(ratio * 100).toFixed(2)}%`
}

export function formatTime(seconds: number): string {
  if (!seconds) return '—'
  const date = new Date(seconds * 1000)
  return Number.isNaN(date.getTime()) ? String(seconds) : date.toLocaleString()
}
