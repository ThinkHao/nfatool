# NFA 95th Web Service (Server)

## 快速开始

1. 安装依赖

```bash
pip install -r server/requirements.txt
```

2. 配置环境变量（可选，支持 `server/.env`）

示例 `server/.env`：
```
API_KEY=your-key
HOST=0.0.0.0
PORT=8000
TIMEZONE=Asia/Shanghai
RETENTION_DAYS=30
CONCURRENCY_LIMIT=3
# 可自定义存储与日志目录
# STORAGE_DIR=c:/path/to/storage
# LOG_DIR=c:/path/to/logs
```

3. 启动

```bash
uvicorn server.main:app --reload --port 8000
```

4. 访问
- Web 页面：`http://127.0.0.1:8000/`
- API 文档：`http://127.0.0.1:8000/docs`

## 说明
- 使用 SQLite 存储任务与运行记录，文件位于 `server/storage/app.db`
- 并发执行限制为 `CONCURRENCY_LIMIT`（默认 3）
- 结果与日志位于 `server/storage/` 与 `server/logs/`
- 清理任务每日 03:30 执行，删除超过 `RETENTION_DAYS` 的运行与产物
- 支持多数据源任务：`data_source_type` 取值 `nfa` / `edc`，并可通过 `data_source_instance` 选择具体数据库实例
- 可通过 `GET /api/meta/data-sources` 查看已配置实例列表（需 API Key）

### 多数据源配置建议

- NFA 任务参数示例：`params.province`、`params.cp`、`params.direction`
- EDC 任务参数示例：`params.edc_name`（支持 `*` 通配），结算可配 `params.settlement_mode`（`range_95` / `daily_95_avg`）
- 在 `.env` 中使用 `NFA_INSTANCES_JSON` / `EDC_INSTANCES_JSON` 配置多实例；若未配置 NFA 实例，仍兼容原 `MYSQL_*` 单实例配置

## 打包为单文件可执行（Windows）

1. 安装依赖（构建机）

```powershell
pip install -r server/requirements.txt
```

2. 一键打包（使用 PyInstaller）

```powershell
powershell -ExecutionPolicy Bypass -File server/build.ps1 -Name nfa95
```

完成后会在 `dist/` 生成 `nfa95.exe`。

3. 分发

- 将 `nfa95.exe` 与一个 `.env` 文件放在同一目录。
- 首次运行会自动在同级目录创建 `logs/` 与 `storage/` 目录。

`.env` 示例（可根据需要调整）：
```
API_KEY=your-key
HOST=0.0.0.0
PORT=8000
TIMEZONE=Asia/Shanghai
RETENTION_DAYS=30
CONCURRENCY_LIMIT=3
# 如需直接连接 MySQL，请填写以下项（推荐在分发时使用这种方式，避免 db_config.ini 文件）：
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=user
MYSQL_PASSWORD=pass
MYSQL_DB=database
```

4. 运行与验证

```powershell
./nfa95.exe
```

- 打开浏览器访问 `http://127.0.0.1:8000/`
- 验证目录：调用 `GET /api/meta/paths`（需带 API Key）查看 `log_dir`、`storage_dir`、`sqlite_url`
- 验证健康：`GET /api/health`

5. 升级

- 停止旧版本进程。
- 只需用新版本 `nfa95.exe` 替换原文件；`.env`、`logs/`、`storage/` 可保留。

Linux 生产环境建议使用“外部升级脚本”模式（更稳健，支持失败回滚）：

1. 将 `server/scripts/nfa95-update.sh` 部署到服务器，例如 `/home/nfa95/bin/nfa95-update.sh`，并授予执行权限。
2. 在 `.env` 配置：
```
UPDATE_EXTERNAL_SCRIPT=/home/nfa95/bin/nfa95-update.sh
UPDATE_SERVICE_NAME=nfa95.service
UPDATE_HEALTHCHECK_URL=http://127.0.0.1:8000/api/health
UPDATE_HEALTHCHECK_TIMEOUT_SEC=45
UPDATE_RUNNER_LOG=/home/nfa95/logs/update-runner.log
```
3. Web 点击“一键升级”后，后端会触发外部脚本执行两阶段切换与健康检查；失败将自动回滚并重启旧版本。

备注：

- 前端静态文件和 `mapping.json` 会随可执行文件一同打包；若需要覆盖，可将 `mapping.json` 放在 exe 同级目录。
- 若未提供 `MYSQL_*`，将回退读取 `db_config.ini`；相对路径会相对可执行文件所在目录解析。

## 下一步
- 抽取 `calculate_95th_percentile.py` 的核心逻辑至 `services/compute95.py`
- 打通即时报表与定时任务的真实计算与导出（CSV/XLSX）
- 下载接口已就绪，真实计算后将看到产物
