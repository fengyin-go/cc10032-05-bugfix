import { createRouter, createWebHistory } from 'vue-router'

import BackfillJobView from '@/views/BackfillJobView.vue'
import LateHandlerView from '@/views/LateHandlerView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 迟到处理与补数任务有专属页面（文件导入导出、覆盖核对），其余模块仍用共享工作台。
    { path: '/late_handler', name: 'late_handler', component: LateHandlerView },
    { path: '/backfill_job', name: 'backfill_job', component: BackfillJobView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
