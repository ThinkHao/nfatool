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

备注：

- 前端静态文件和 `mapping.json` 会随可执行文件一同打包；若需要覆盖，可将 `mapping.json` 放在 exe 同级目录。
- 若未提供 `MYSQL_*`，将回退读取 `db_config.ini`；相对路径会相对可执行文件所在目录解析。

## 下一步
- 抽取 `calculate_95th_percentile.py` 的核心逻辑至 `services/compute95.py`
- 打通即时报表与定时任务的真实计算与导出（CSV/XLSX）
- 下载接口已就绪，真实计算后将看到产物
