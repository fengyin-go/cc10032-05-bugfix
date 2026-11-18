# 实时计算服务中心

面向实时数据分析场景，提供流式计算定义、窗口聚合、策略告警和状态管理的实时数据平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  大部分模块共享工作台
│   ├── src/views/LateHandlerView.vue    迟到处理专属页（导入/回执/导出）
│   ├── src/views/BackfillJobView.vue    补数任务专属页（区段覆盖/清单下载）
│   ├── src/domain/lateBypass.ts         迟到旁路与补数任务的接口封装
│   ├── src/data/modules.ts              业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/routers/late.py      迟到处理/补数任务的文件接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/late_bypass.py  迟到旁路、区段切分与清单校验的共享逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流式源端 | `stream_source` | 数据源配置 | 源端名称、消息队列、Topic名称 |
| 窗口配置 | `window_config` | 窗口策略 | 策略名称、窗口类型、窗口大小 |
| 计算算子 | `calc_operator` | 算子定义 | 算子名称、算子类型、输入字段 |
| 状态存储 | `state_store` | 状态配置 | 配置名称、后端类型、状态键模式 |
| 水位追踪 | `watermark_track` | 水位记录 | 水位名称、数据源、当前水位 |
| 告警策略 | `alert_policy` | 告警规则 | 规则名称、监控指标、触发条件 |
| 滑动指标 | `sliding_index` | 指标计算 | 指标名称、计算窗口、聚合函数 |
| 流关联 | `join_stream` | 关联配置 | 配置名称、左流源、右流源 |
| 结果落地 | `sink_output` | 输出配置 | 配置名称、输出类型、目标地址 |
| 补数任务 | `backfill_job` | 补数作业 | 作业名称、目标指标、补数范围 |
| CEP规则 | `cep_rule` | 事件序列 | 序列名称、事件类型、匹配顺序 |
| 吞吐管理 | `throughput_mgr` | 吞吐配置 | 配置名称、数据源、最大QPS |
| 检查点 | `checkpoint_mgr` | 检查点配置 | 配置名称、间隔时间、超时时长 |
| 故障恢复 | `restore_plan` | 恢复方案 | 方案名称、任务范围、恢复模式 |
| 指标血缘 | `metric_lineage` | 血缘记录 | 记录ID、指标名称、上游源端 |
| 迟到处理 | `late_handler` | 处理策略 | 策略名称、允许延迟、迟到动作 |
| 任务拓扑 | `task_topology` | 拓扑配置 | 拓扑名称、算子链、并行度 |
| 数据采样 | `data_sampler` | 采样策略 | 策略名称、采样方式、采样比例 |
| 调试追踪 | `debug_trace` | 调试会话 | 会话名称、目标算子、采样数据 |
| 聚合落库 | `aggregate_sink` | 聚合任务 | 任务名称、源指标集、聚合粒度 |

## 约定

- 大部分模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由；
  迟到处理与补数任务有专属页面，文件导入导出和覆盖核对走
  `backend/app/routers/late.py`。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 迟到处理 → 补数任务

这条链路是同一个根因的两端，改动时两边要一起看：

```text
迟到事件（CSV/JSON）
  └─ 判定：event_time < 水位 - 允许延迟（相等算准时）
      └─ 阈值：late_ratio > 阈值百分比 / 100（相等不触发）
          └─ 迟到旁路数据：按源端 + 事件时间切区段，带版本
              └─ 补数作业：每个未覆盖区段一条可执行作业
                  └─ 导出清单：迟到明细 + 区段 + 作业，可回传校验
```

- 阈值取自策略「阈值百分比」，按百分数解析（`5` 就是 5%）。配置为空或
  不是 0-100 的数值时回退策略默认值 5%，不会猜成 0.1 之类的小比例。
- 只有越过阈值才写旁路数据和补数作业；未越过阈值只留批量回执，页面能看到
  本批实际采用的比例与阈值。
- 同一份文件重复导入只生效一次；同一事件在第二份文件边界里再出现也不重复计数。
- 同一迟到范围只保留一条可执行作业：再次越过阈值扩展原作业范围并追加进度；
  暂停时可选扩大原作业（`extend`）或关闭旧作业新建分段作业（`split`）。
- 导出清单（`GET /api/late_handler/{策略}/export`）记录校验和；其它页面回传时
  先校验覆盖与明细，再幂等入库，校验不通过则整批退回、不改动原数据。

主要接口：

| 用途 | 接口 |
| --- | --- |
| 读取迟到事件并生成作业 | `POST /api/late_handler/{策略}/ingest` |
| 迟到处理页数据（实际阈值/比例/覆盖） | `GET /api/late_handler/{策略}/processing` |
| 导出 / 校验 / 接收同源清单 | `GET .../export`、`POST .../verify`、`POST .../import` |
| 补数页面（区段 × 作业覆盖） | `GET /api/backfill_job/console` |
| 补数清单下载 / 校验 | `GET /api/backfill_job/manifest`、`POST /api/backfill_job/verify` |
| 暂停作业分段重建 | `POST /api/backfill_job/{id}/split` |

领域：数据工程
子方向：数据分析与数据应用
