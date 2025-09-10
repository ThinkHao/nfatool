# nfa_ipgroup_autofill 使用说明

本工具用于按既定规则自动补全 `nfa_ipgroup` 表中新增或不完整的记录字段，支持 dry-run 预览、按 `nfa_uuid` 过滤范围，并输出仍需人工处理的“置空字段”清单。

- 脚本文件：`nfa_ipgroup_autofill.py`
- 依赖环境：Python 3.8+、`pymysql`
- 配置文件：`db_config.ini`
- CP 映射：`mapping.json`

---

## 1. 适用场景

当 `nfa_ipgroup` 表中出现新增数据或存在字段待填时，可使用本工具依据既有规则自动补全如下字段：

- `check_status`
- `type`
- `nfa_name`
- `school_name`
- `cp`
- `region`
- `school_id`
- `saler_group`
- `saler`

工具将尽量利用历史数据与命令行参数进行补全，同时输出仍无法确定的字段，便于人工后续处理。

---

## 2. 字段填充规则（优先级）

- `check_status`
  - 若非 0，统一置为 `0`。

- `type`
  - 若非 `yuanxiao`，统一置为 `yuanxiao`。

- `nfa_name`
  - 优先：使用相同 `nfa_uuid` 的历史记录中的 `nfa_name`。
  - 其次：若无历史，则使用命令行参数 `--nfa-name` 的值。
  - 两者都没有：置空，并在日志“置空字段”中输出。

- `ipgroup_name` 派生 `school_name`、`cp`
  - 命名格式：`院校名称_CP显示名_IP版本`。
  - 兼容 IP 版本后缀：`V4`/`V6`、`V4-1`/`V6-2`（含中英文短横/破折号）。内部规范化为 `V4` 或 `V6`。
  - 仅提取中间段 `CP显示名`，再通过 `mapping.json` 映射为简称写入 `cp`（例如：`新流` → `xinliu`）。不会拼接省份。
  - 若无法解析出 `school_name` 或 `cp`，或 `cp` 无法映射到简称，则将对应字段加入“置空字段”。

- `region`
  - 优先：命令行参数 `--region`。
  - 其次：沿用相同 `nfa_uuid` 的历史记录。
  - 仍无：置空，并在日志“置空字段”中输出。

- `school_id`
  - 根据 `school_name` 到历史记录中查找最近（按 `update_time/create_time`）的非空 `school_id` 并沿用。
  - 若未找到：置空，并在日志“置空字段”中输出。

- `saler_group`、`saler`
  - 优先：根据 `school_name` 在历史记录中沿用。
  - 其次：命令行参数 `--saler-group`、`--saler`。
  - 都无：置空，并在日志“置空字段”中输出。

---

## 3. 依赖与准备

- `db_config.ini`（数据库配置）
  ```ini
  [DATABASE]
  host = 127.0.0.1
  port = 3306
  user = your_user
  password = your_password
  db = your_database
  charset = utf8mb4
  ```

- `mapping.json`（CP显示名 → 简称）
  ```json
  {
    "白山": "bsy",
    "B站": "bilibili",
    "网宿": "cnc",
    "新流": "xinliu",
    "阿里": "ali",
    "金山": "jinshan",
    "百度": "baidu"
  }
  ```
  如有新的 CP 显示名，请补充映射后再运行工具。

- Python 依赖
  ```bash
  pip install pymysql
  ```

---

## 4. 命令行参数

- `--config`：数据库配置文件路径，默认 `db_config.ini`。
- `--mapping`：CP 显示名映射文件路径，默认 `mapping.json`。
- `--nfa-uuid`：限制操作范围的 `nfa_uuid`，支持逗号分隔多个。
- `--region`：当目标记录的 `region` 为空时优先使用该参数。
- `--nfa-name`：当无历史 `nfa_name` 可沿用时用于回退填充。
- `--saler-group` / `--saler`：当无历史可沿用时用于回退填充。
- `--execute`：实际写库。若不提供，则为 dry-run（仅预览，不写库）。

---

## 5. 使用示例（Windows PowerShell）

- 仅按 `nfa_uuid` 预览（dry-run，不写库）：
  ```powershell
  python .\nfa_ipgroup_autofill.py --config db_config.ini --mapping mapping.json `
    --nfa-uuid ab048421-f121-38b4-b6a9-da05ca43db7b
  ```

- 指定 `region` 与 `nfa_name` 回退（dry-run）：
  ```powershell
  python .\nfa_ipgroup_autofill.py --config db_config.ini --mapping mapping.json `
    --nfa-uuid ab048421-f121-38b4-b6a9-da05ca43db7b `
    --region 广东省 `
    --nfa-name "NFA名称占位"
  ```

- 提供 `saler_group`、`saler` 回退（dry-run）：
  ```powershell
  python .\nfa_ipgroup_autofill.py --config db_config.ini --mapping mapping.json `
    --nfa-uuid ab048421-f121-38b4-b6a9-da05ca43db7b `
    --saler-group 校园组A --saler 张三
  ```

- 实际执行（写库）：
  ```powershell
  python .\nfa_ipgroup_autofill.py --config db_config.ini --mapping mapping.json `
    --nfa-uuid ab048421-f121-38b4-b6a9-da05ca43db7b --execute
  ```

- 多个 `nfa_uuid`（逗号分隔）：
  ```powershell
  python .\nfa_ipgroup_autofill.py --config db_config.ini --mapping mapping.json `
    --nfa-uuid uuid1,uuid2,uuid3
  ```

---

## 6. 日志说明

- dry-run 将输出形如：
  ```text
  [dry-run] id=334014624 ipgroup='院校017_新流_V4' 将更新: cp: 'None' -> 'xinliu'; type: 'None' -> 'yuanxiao'; check_status: 'None' -> '0'; ...
  ```

- 同时输出仍未能自动填充的字段：
  ```text
  以下记录存在仍未能自动填充的字段，请人工后续处理：
    id=334014624 ipgroup_name='院校017_新流_V4' 空字段: region, nfa_name
  ```

- 实际执行模式会汇总提交条数，每 500 条提交一次，失败时对失败记录进行逐条错误日志输出。

---

## 7. 常见问题（FAQ）

- Q: `ipgroup_name` 不规范，比如不是 `院校名称_CP显示名_IP版本` 格式怎么办？
  - A: 无法从中解析 `school_name`/`cp` 时，这些字段会被列入“置空字段”。请在 DB 中修正 `ipgroup_name` 或手动填写后再运行。

- Q: `cp` 无法映射到简称？
  - A: 请在 `mapping.json` 中补充对应 `CP显示名 → 简称` 的映射。例如 `"新流": "xinliu"`。

- Q: 是否会覆盖已有值？
  - A: 脚本只会在字段为空或不符合规则时进行填充/调整，例如 `type` 非 `yuanxiao` 会被统一改为 `yuanxiao`，`check_status` 非 0 会被改为 0。

- Q: `region` 未传参数时如何处理？
  - A: 若能从同 `nfa_uuid` 历史记录沿用则使用，否则置空并输出提示。

---

## 8. 安全与回滚建议

- 强烈建议先以 **dry-run** 预览，确认“将更新”与“置空字段”列表无误后再加 `--execute` 实际写库。
- 建议在生产环境执行前对相关表做数据库备份或开启事务快照，以便回滚。

---

## 9. 版本说明

- 当前版本：兼容 `V4/V6` 与 `V4-1/V6-2` 后缀（含中英文短横/破折号），规范化为 `V4`/`V6`，仅提取中段 `CP显示名` 并按映射写入 `cp`。

如需扩展更多命名兼容形式（例如 `V4_1`、`V4(1)`），可在 `parse_ipgroup_name()` 中增加规则。
