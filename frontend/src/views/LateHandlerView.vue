<template>
  <section class="page" data-module="late_handler">
    <header class="page-head">
      <div>
        <h2>迟到处理</h2>
        <p class="page-desc">
          读取迟到事件文件、按策略阈值判断是否越过阈值，并为每个异常区段生成一条补数作业。
          阈值取「阈值百分比」；配置为空时回退策略默认值 5%，不会猜成别的小数。
        </p>
      </div>
      <div class="page-actions">
        <select v-model="policy" data-testid="policy-select" class="policy-select" @change="reload">
          <option v-for="item in policies" :key="String(item.策略名称)" :value="String(item.策略名称)">
            {{ item.策略名称 }}（{{ item.status }}）
          </option>
        </select>
        <button class="btn" data-testid="refresh" type="button" @click="reload">刷新</button>
        <RouterLink class="btn" to="/backfill_job">查看补数任务</RouterLink>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="error">{{ errorMessage }}</p>
    <p v-if="notice" class="notice-text" data-testid="notice">{{ notice }}</p>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">实际采用阈值</span>
        <strong class="stat-value" data-testid="threshold">{{ thresholdText }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">本批迟到比例</span>
        <strong class="stat-value" data-testid="late-ratio">{{ lateRatioText }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">异常区段 / 已覆盖</span>
        <strong class="stat-value" data-testid="segment-coverage">
          {{ detail?.coverage.covered ?? 0 }} / {{ detail?.coverage.segment_count ?? 0 }}
        </strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">旁路明细</span>
        <strong class="stat-value" data-testid="bypass-count">{{ detail?.bypass_count ?? 0 }}</strong>
      </article>
    </div>

    <p class="status-legend" data-testid="threshold-source">
      阈值来源：{{ detail?.settings.threshold_source ?? '—' }}；允许延迟：{{ detail?.settings.allowed_lateness ?? '—' }} 秒
    </p>

    <section class="panel">
      <h3>读取迟到事件</h3>
      <div class="upload-row">
        <input
          ref="fileInput"
          data-testid="late-file"
          type="file"
          accept=".csv,.json,.txt,text/csv,application/json"
          @change="onFileSelected"
        />
        <select v-model="pausedStrategy" data-testid="paused-strategy" class="policy-select">
          <option value="extend">暂停中：扩大原作业范围</option>
          <option value="split">暂停中：关闭旧作业并新建分段作业</option>
        </select>
        <button class="btn primary" data-testid="ingest" type="button" :disabled="!fileContent || busy" @click="ingestFile">
          {{ busy ? '处理中…' : '导入并生成补数任务' }}
        </button>
      </div>
      <p class="hint">
        支持 CSV（表头：事件时间/源端/事件ID）和 JSON；也支持其它页面回传的同源导出清单。
        同一份文件重复导入只生效一次。
      </p>
      <textarea
        v-model="fileContent"
        data-testid="late-content"
        class="content-box"
        placeholder="也可以直接粘贴文件内容"
      ></textarea>
    </section>

    <section class="panel">
      <h3>批量回执</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>批次号</th><th>文件名</th><th>总事件</th><th>迟到数</th>
            <th>迟到比例</th><th>阈值</th><th>阈值来源</th><th>区段数</th><th>作业</th><th>重复</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="batch in detail?.batches ?? []" :key="batch.批次号" :data-testid="`batch-${batch.批次号}`">
            <td>{{ batch.批次号 }}</td>
            <td>{{ batch.文件名 || '—' }}</td>
            <td>{{ batch.总事件数 }}</td>
            <td>{{ batch.迟到数 }}</td>
            <td>{{ formatPercent(batch.迟到比例) }}</td>
            <td>{{ batch.阈值百分比 }}%</td>
            <td>{{ batch.阈值来源 }}</td>
            <td>{{ batch.区段数 }}</td>
            <td>{{ batch.作业.join('、') || '—' }}</td>
            <td>{{ batch.幂等重复 ? '是' : '否' }}</td>
          </tr>
          <tr v-if="!(detail?.batches ?? []).length">
            <td colspan="10" class="empty-state">还没有导入记录</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section class="panel">
      <h3>迟到区段与补数作业</h3>
      <table class="data-table">
        <thead>
          <tr><th>区段</th><th>源端</th><th>版本</th><th>迟到事件</th><th>补数范围</th><th>作业</th><th>状态</th><th>进度</th></tr>
        </thead>
        <tbody>
          <tr v-for="segment in detail?.segments ?? []" :key="segment.segment_id" :data-testid="`segment-${segment.segment_id}`">
            <td>{{ segment.segment_id }}</td>
            <td>{{ segment.source }}</td>
            <td>{{ segment.version }}</td>
            <td>{{ segment.late_count }}</td>
            <td>{{ segment.range }}</td>
            <td>{{ segment.jobs.map((job) => job.job_name).join('、') || '未生成' }}</td>
            <td>{{ segment.jobs.map((job) => job.status).join('、') || '—' }}</td>
            <td>{{ segment.jobs.map((job) => job.progress).join('、') || '—' }}</td>
          </tr>
          <tr v-if="!(detail?.segments ?? []).length">
            <td colspan="8" class="empty-state">暂无迟到区段数据</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section class="panel">
      <h3>导出与校验</h3>
      <div class="upload-row">
        <button class="btn" data-testid="export-manifest" type="button" @click="exportManifest">
          导出迟到旁路数据 + 补数清单
        </button>
        <button class="btn" data-testid="export-detail" type="button" @click="exportCsv(lateDetailCsvPath(policy), 'late_detail.csv')">
          导出迟到明细
        </button>
        <button class="btn" data-testid="export-batches" type="button" @click="exportCsv(lateBatchesCsvPath(policy), 'late_batches.csv')">
          导出批量回执
        </button>
        <button class="btn" data-testid="import-same-source" type="button" :disabled="!fileContent || busy" @click="importManifest">
          接收同源导出文件
        </button>
      </div>
      <p v-if="verifyResult" class="hint" data-testid="verify-result">
        校验结果：{{ verifyResult.ok ? '通过' : '未通过' }}
        <span v-if="verifyResult.issues.length">；{{ verifyResult.issues.join('；') }}</span>
      </p>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'

import {
  downloadFile,
  fetchLateProcessing,
  formatPercent,
  importSameSourceFile,
  ingestLateFile,
  lateBatchesCsvPath,
  lateDetailCsvPath,
  lateManifestPath,
  readFileText,
  verifyLateFile,
  type LateProcessingDetail,
  type VerifyResult,
} from '@/domain/lateBypass'
import { listEntries } from '@/api/backend-service'

const route = useRoute()
const policies = ref<Record<string, string | number | boolean>[]>([])
const policy = ref<string>(String(route.query.policy ?? ''))
const detail = ref<LateProcessingDetail | null>(null)
const errorMessage = ref('')
const notice = ref('')
const busy = ref(false)
const fileContent = ref('')
const fileName = ref('')
const pausedStrategy = ref<'extend' | 'split'>('extend')
const verifyResult = ref<VerifyResult | null>(null)
const fileInput = ref<HTMLInputElement | null>(null)

const thresholdText = computed(() => {
  if (!detail.value) return '—'
  const { threshold_percent, threshold_ratio, threshold_fallback } = detail.value.settings
  return `${threshold_percent}%（${threshold_ratio}${threshold_fallback ? '，默认值' : ''}）`
})
const lateRatioText = computed(() => {
  if (!detail.value) return '—'
  return formatPercent(detail.value.actual_late_ratio)
})

async function loadPolicies() {
  const payload = await listEntries('late_handler')
  policies.value = payload.items as unknown as Record<string, string | number | boolean>[]
  if (!policy.value && policies.value.length) {
    policy.value = String(policies.value[0].策略名称)
  }
}

async function reload() {
  errorMessage.value = ''
  if (!policy.value) return
  try {
    detail.value = await fetchLateProcessing(policy.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '迟到处理详情读取失败'
  }
}

async function onFileSelected(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  if (!file) return
  errorMessage.value = ''
  try {
    fileContent.value = await readFileText(file)
    fileName.value = file.name
    verifyResult.value = null
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '文件读取失败'
  }
}

async function ingestFile() {
  if (!policy.value) return
  busy.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    const result = await ingestLateFile(policy.value, fileContent.value, fileName.value, pausedStrategy.value)
    notice.value = result.duplicate
      ? '同一份文件已导入过，本次不重复入库'
      : result.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导入失败'
  } finally {
    busy.value = false
  }
}

async function importManifest() {
  if (!policy.value) return
  busy.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    verifyResult.value = await verifyLateFile(policy.value, fileContent.value)
    const result = await importSameSourceFile(policy.value, fileContent.value, fileName.value, pausedStrategy.value)
    notice.value = result.duplicate ? '同源导出文件已接收过，本次不重复入库' : result.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '同源文件接收失败'
  } finally {
    busy.value = false
  }
}

async function exportManifest() {
  errorMessage.value = ''
  try {
    await downloadFile(lateManifestPath(policy.value), `${policy.value}_late_manifest.json`)
    notice.value = '已导出迟到旁路数据与补数任务清单'
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

async function exportCsv(path: string, filename: string) {
  errorMessage.value = ''
  try {
    await downloadFile(path, filename)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

onMounted(async () => {
  try {
    await loadPolicies()
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '迟到处理页面初始化失败'
  }
})
</script>

<style scoped>
.panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; margin-bottom: 12px; }
.panel h3 { margin: 0 0 10px; font-size: 14px; }
.upload-row { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin-bottom: 8px; }
.policy-select { border: 1px solid var(--border); border-radius: 6px; padding: 6px 10px; }
.content-box { width: 100%; min-height: 96px; border: 1px solid var(--border); border-radius: 6px; padding: 8px; font-family: monospace; font-size: 12px; }
.hint { color: var(--muted); font-size: 12px; margin: 4px 0 8px; }
.notice-text { color: #1d7a46; font-size: 13px; }
</style>
