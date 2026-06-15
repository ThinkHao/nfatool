# nfatool — Claude 项目说明

NFA / EDC 95 分位（95th percentile）带宽结算工具。核心是一个 FastAPI 服务，带 Web 页面用于创建/调度计算任务，并把结果导出为 CSV/XLSX。另有 Electron / 托盘（tray）外壳和自动更新流水线。

## 目录速览

- `server/` — 后端服务（FastAPI）+ Web 页面，**日常开发主要在这里**。
  - `server/main.py` — 应用入口（`uvicorn server.main:app`）。
  - `server/services/compute95.py` — 核心计算：NFA / EDC 的 95 分位按天/区间/按月聚合与导出。
  - `server/services/scheduler.py` — 定时任务调度。
  - `server/ext/calculate_95th_percentile.py` — 原始计算逻辑（被 compute95 复用）。
  - `server/config.py` — 配置与数据源实例解析（env JSON 或运行时文件）。
  - `server/static/index.html` + `server/static/app.js` — Web 页面（Vue，CDN 引入，无构建步骤）。
  - `server/storage/` — SQLite (`app.db`)、产物、`data_source_instances.json`。
  - `server/tests/` — pytest 测试。
  - `server/README.md` — 部署、打包(PyInstaller)、双仓发版等运维细节。
- `electron/`、`gui/`、`tray_*.py` — 桌面/托盘外壳（非核心计算逻辑）。

## 运行 / 测试

```bash
pip install -r server/requirements.txt
uvicorn server.main:app --reload --port 8000   # Web: http://127.0.0.1:8000/  Docs: /docs
python -m pytest server/tests/ -q              # 测试（须从仓库根目录运行，包用 server.* 绝对导入）
```

> 注意：`compute95.py` 用相对导入 `from ..ext import ...`，直接 `cd server` 跑脚本会报 “relative import beyond top-level package”。一律从**仓库根目录**以 `server.services.compute95` 形式导入/运行。

## 数据源（NFA / EDC）

- 任务有 `data_source_type`（`nfa` / `edc`）和 `data_source_instance`（选具体数据库实例）。
- 实例配置来源：`.env` 的 `NFA_INSTANCES_JSON` / `EDC_INSTANCES_JSON`，或运行时文件 `server/storage/data_source_instances.json`。NFA 未配实例时回退兼容旧 `MYSQL_*` 单实例。
- 实例字段：`table` / `time_column` / `name_column` / `value_column` / `exclude_like` / `wildcard_mode` / `daily_rank_index`，可选 SSH 隧道 `ssh_*`。
- `GET /api/meta/data-sources` 查看已配置实例。

### EDC 名称匹配（prefix / exact / IN）

`compute95.py` 中由 `_edc_name_predicate()` / `_parse_edc_names()` 构建 `name_col` 的 WHERE 片段（全部参数化，禁止字符串拼接值）：

- `params.edc_match_mode` 覆盖实例的 `wildcard_mode`：`prefix`（默认，`x` → `LIKE 'x%'`）或 `exact`（等值）。
- 含 `*`/`?` 的 token 始终走 glob（`*`→`%`，`?`→`_`）。
- `edc_name` 可用逗号/换行分隔多个名称：
  - 多个**精确**名 → 合并成单个 `name_col IN (%s, ...)`（索引友好的等值探测），可精确包含指定节点而不误命中相邻前缀（如 `SD-cs-bj-3495,SD-cs-bj-3496` 不命中 `3497`）。
  - 多个前缀/glob → 用 `OR` 连接的 `LIKE`。
  - 单个名称与改造前行为完全一致。
- 名称数量上限 `EDC_MAX_NAMES = 500`（超限抛错，避免超大 IN 拖垮优化器）。
- 前端：创建/编辑任务表单有“前缀(prefix) / 精确(exact/IN)”下拉；`edc_name` 输入框支持逗号分隔。

### EDC 表性能注意（实例 `local` = `traffic_5m`）

- `traffic_5m` 约 4500 万行，**唯一索引是主键 `(create_time, cds_sn, sn, en_type)`，`edc_name` 上无索引**。
- 查询靠主键前缀 `create_time` 做范围扫描，再在时间窗内对 `edc_name` 行级过滤。因此 `IN` vs `LIKE` 性能基本无差异——瓶颈是时间窗行数，不是匹配方式。
- 实现 EDC 查询时**务必保留 `create_time` 范围条件**；无时间约束的全表统计（如 `COUNT(*)`）会超时。
- 若按 `edc_name` 过滤变得频繁/沉重，可考虑加二级索引 `(name_column, time_column)`（属独立优化，非匹配改造前提）。

## 约定

- 计算任务参数放在 `params.*`；NFA 用 `province`/`cp`/`direction`，EDC 用 `edc_name`/`edc_match_mode`/`settlement_mode` 等。
- EDC 5 分钟点位换算用 `*8/300`（NFA 用 `*8/60`）。
- SQL 标识符（表/列名）经 `_safe_identifier()` 白名单校验；值一律用占位符参数化。
