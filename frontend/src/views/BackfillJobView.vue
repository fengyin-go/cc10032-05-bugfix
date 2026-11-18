<template>
  <section class="page" data-module="backfill_job">
    <header class="page-head">
      <div>
        <h2>补数任务</h2>
        <p class="page-desc">
          作业范围来自迟到处理写入的迟到旁路数据：每个异常区段只对应一条可执行作业。
          同一区段再次越过阈值只会在原作业上追加进度，暂停后可扩大范围或分段重建。
        </p>
      </div>
      <div class="page-actions">
        <select v-model="policy" data-testid="policy-filter" class="policy-select" @change="reload">
          <option value="">全部策略</option>
          <option v-for="name in policyNames" :key="name" :value="name">{{ name }}</option>
        </select>
        <button class="btn" data-testid="refresh" type="button" @click="reload">刷新</button>
        <RouterLink class="btn" to="/late_handler">回到迟到处理</RouterLink>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="error">{{ errorMessage }}</p>
    <p v-if="notice" class="notice-text" data-testid="notice">{{ notice }}</p>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">异常区段</span>
        <strong class="stat-value" data-testid="segment-total">{{ filteredSegments.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已有作业</span>
        <strong class="stat-value" data-testid="job-total">{{ filteredJobs.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">未覆盖区段</span>
        <strong class="stat-value" data-testid="uncovered-total">{{ uncovered.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">可执行范围</span>
        <strong class="stat-value" data-testid="executable-total">{{ executableCount }}</strong>
      </article>
    </div>

    <p class="status-legend" data-testid="coverage-summary">
      覆盖结论：{{ uncovered.length ? `有 ${uncovered.length} 个区段缺少可执行作业` : '每个异常区段都有作业' }}
    </p>

    <section class="panel">
      <h3>异常区段 → 补数作业</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>区段</th><th>源端</th><th>补数范围</th><th>迟到事件</th><th>作业 ID</th>
            <th>作业名称</th><th>状态</th><th>进度</th><th>覆盖</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="segment in filteredSegments"
            :key="segment.segment_id"
            :data-testid="`segment-${segment.segment_id}`"
          >
            <td>{{ segment.segment_id }}</td>
            <td>{{ segment.source }}</td>
            <td>{{ segment.range }}</td>
            <td>{{ segment.late_count }}</td>
            <td>{{ segment.job_id ?? '—' }}</td>
            <td>{{ segment.jobs.map((job) => job.job_name).join('、') || '—' }}</td>
            <td>{{ segment.jobs.map((job) => (job.paused ? `${job.status}（暂停）` : job.status)).join('、') || '—' }}</td>
            <td>{{ segment.jobs.map((job) => job.progress).join('、') || '—' }}</td>
            <td :data-covered="segment.covered">{{ segment.covered ? '已覆盖' : '未覆盖' }}</td>
            <td class="row-actions">
              <button
                v-for="job in segment.jobs"
                :key="job.job_id"
                class="link"
                type="button"
                :data-testid="`start-${job.job_id}`"
                @click="runJobAction(job.job_id, '启动补数')"
              >
                启动 {{ job.job_id }}
              </button>
              <button
                v-for="job in segment.jobs"
                :key="`pause-${job.job_id}`"
                class="link"
                type="button"
                :data-testid="`pause-${job.job_id}`"
                @click="runJobAction(job.job_id, '暂停补数')"
              >
                暂停 {{ job.job_id }}
              </button>
              <button
                v-for="job in segment.jobs.filter((item) => item.paused)"
                :key="`split-${job.job_id}`"
                class="link"
                type="button"
                :data-testid="`split-${job.job_id}`"
                @click="splitJob(job.job_id)"
              >
                分段重建 {{ job.job_id }}
              </button>
            </td>
          </tr>
          <tr v-if="!filteredSegments.length">
            <td colspan="10" class="empty-state">暂无迟到区段数据，先在迟到处理页面导入迟到事件</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section class="panel">
      <h3>导出与校验</h3>
      <div class="upload-row">
        <button class="btn" data-testid="download-manifest" type="button" @click="downloadManifest">
          下载补数任务清单
        </button>
        <button class="btn" data-testid="download-tasks" type="button" @click="downloadTasks">
          下载任务清单（CSV）
        </button>
        <input
          data-testid="manifest-file"
          type="file"
          accept=".json,.csv,.txt,application/json,text/csv"
          @change="onManifestSelected"
        />
      </div>
      <p v-if="verifyResult" class="hint" data-testid="verify-result">
        清单校验：{{ verifyResult.ok ? '通过' : '未通过' }}
        <span v-if="verifyResult.issues.length">；{{ verifyResult.issues.join('；') }}</span>
        <span v-if="verifyResult.backfill">
          ；区段 {{ verifyResult.backfill.segment_count }} 个，作业 {{ verifyResult.backfill.job_count }} 条
        </span>
      </p>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { runAction } from '@/api/backend-service'
import {
  backfillManifestPath,
  backfillTasksCsvPath,
  downloadFile,
  fetchBackfillConsole,
  readFileText,
  splitPausedJob,
  verifyBackfillManifest,
  type BackfillConsole,
  type BackfillSegment,
  type LateJob,
  type VerifyResult,
} from '@/domain/lateBypass'

const console = ref<BackfillConsole | null>(null)
const policy = ref('')
const errorMessage = ref('')
const notice = ref('')
const verifyResult = ref<VerifyResult | null>(null)

const policyNames = computed(() => Object.keys(console.value?.policies ?? {}).sort())
const selected = computed(() => (policy.value ? console.value?.policies[policy.value] : undefined))
const filteredSegments = computed<BackfillSegment[]>(() => {
  if (selected.value) return selected.value.segments
  return Object.values(console.value?.policies ?? {}).flatMap((item) => item.segments)
})
const filteredJobs = computed<LateJob[]>(() => {
  if (selected.value) return selected.value.jobs
  return Object.values(console.value?.policies ?? {}).flatMap((item) => item.jobs)
})
const uncovered = computed(() =>
  filteredSegments.value.filter((segment) => !segment.covered).map((segment) => segment.segment_id),
)
const executableCount = computed(() =>
  filteredSegments.value.reduce(
    (total, segment) => total + segment.jobs.filter((job) => job.executable).length,
    0,
  ),
)

async function reload() {
  errorMessage.value = ''
  try {
    console.value = await fetchBackfillConsole()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补数任务读取失败'
  }
}

async function runJobAction(jobId: number, action: string) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const result = await runAction('backfill_job', jobId, action)
    if (!result.ok) throw new Error(result.message)
    notice.value = result.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '作业操作失败'
  }
}

async function splitJob(jobId: number) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const result = await splitPausedJob(jobId)
    notice.value = result.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分段重建失败'
  }
}

async function downloadManifest() {
  errorMessage.value = ''
  try {
    const target = policy.value || null
    await downloadFile(backfillManifestPath(target), target ? `${target}_backfill_manifest.json` : 'backfill_manifest.json')
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '下载补数任务清单失败'
  }
}

async function downloadTasks() {
  errorMessage.value = ''
  try {
    await downloadFile(backfillTasksCsvPath(policy.value || null), 'backfill_tasks.csv')
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '下载任务清单失败'
  }
}

async function onManifestSelected(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  if (!file) return
  errorMessage.value = ''
  verifyResult.value = null
  try {
    const content = await readFileText(file)
    verifyResult.value = await verifyBackfillManifest(content)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '清单校验失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; margin-bottom: 12px; }
.panel h3 { margin: 0 0 10px; font-size: 14px; }
.upload-row { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; }
.policy-select { border: 1px solid var(--border); border-radius: 6px; padding: 6px 10px; }
.hint { color: var(--muted); font-size: 12px; margin: 8px 0 0; }
.notice-text { color: #1d7a46; font-size: 13px; }
</style>
