# 95值计算工具使用说明

## 功能简介

本工具用于计算指定省份、指定CP类型、指定时间范围内所有院校的95值。95值的计算方式为：将一段时间内的流速，按照从大到小排序，舍弃前5%的点，取剩下的最大的点。

流量单位换算方式：send 和 recv 的单位是 bytes，转换为 Mbps 的公式为：Mbps = (bytes * 8 / 60 / 1024 / 1024)

## 数据来源

工具基于以下两张数据表：
- `nfa_ipgroup`：关系表，包含院校信息、区域、CP类型等
- `nfa_ip_group_speed_logs_5m`：流速表，包含每5分钟的流量数据

## 功能特点

1. 根据指定的省份和CP类型筛选院校
2. 获取指定时间范围内的流速数据
3. 按照95计算方式计算95值
4. 支持选择流量方向（发送、接收或双向）
5. 将结果输出到CSV文件
6. 提供汇总统计信息 (当计算周期汇总95值时)
7. 支持导出每日95值数据
8. 支持按院校名单进行“排除院校单独计算 + 剩余院校单独计算/汇总”
9. 默认仅统计 `type = 'yuanxiao'` 的院校，自动排除 `type = 'dianbo'`（点播）与 `type = 'zhibo'`（直播）

## 安装依赖

本工具依赖以下Python库：
```bash
pip install pymysql pandas numpy
```

## 配置数据库

使用前需要先配置数据库连接信息。编辑`db_config.ini`文件：

```ini
[DATABASE]
host = 数据库主机地址
port = 3306
user = 用户名
password = 密码
db = 数据库名
charset = utf8mb4
```

## 使用方法

### 基本用法

```bash
python calculate_95th_percentile.py --province 省份 --cp CP类型 --start-time "开始时间" --end-time "结束时间"
```

> 说明：自 版本更新 起，程序在筛选 `nfa_ipgroup` 时会默认加上 `type = 'yuanxiao'` 条件，仅统计真实院校条目；`type` 为 `dianbo`（点播）或 `zhibo`（直播）的条目将被排除。

### 参数说明

| 参数 | 简写 | 说明 | 是否必需 | 默认值 |
|------|------|------|----------|--------|
| `--province` | `-p` | 指定省份，例如：四川省 | 是 | - |
| `--cp` | `-c` | 指定CP类型，例如：bilibili | 是 | - |
| `--start-time` | `-s` | 开始时间，格式：YYYY-MM-DD HH:MM:SS | 是 | - |
| `--end-time` | `-e` | 结束时间，格式：YYYY-MM-DD HH:MM:SS | 是 | - |
| `--direction` | `-d` | 流量方向：send(发送)、recv(接收)或both(双向) | 否 | both |
| `--output` | `-o` | 输出结果文件路径 | 否 | 95th_percentile_results.csv |
| `--config` | - | 数据库配置文件路径 | 否 | db_config.ini |
| `--school` | `-sc` | 指定院校名称，多个院校用逗号分隔，例如：电子科技大学,四川大学 | 否 | - |
| `--export-daily` |  | 导出每日95值，而不是整个周期的汇总95值。当使用此参数时，输出文件将包含每日数据，且不提供控制台汇总统计。 | 否 | False (开关参数) |
| `--exclude-school` | `-esc` | 排除的院校名称，多个院校用逗号分隔。将对“排除名单院校”（逐校）与“剩余院校”（整体汇总）分别计算并分别输出。剩余院校会先在每个时间点上把 recv/send 求和后再计算95值。 | 否 | - |
| `--sortby` |  | 按该字段排序输出，例如：95th_percentile_mbps、daily_95th_percentile_mbps、ipgroup_name 等 | 否 | - |
| `--sort-order` |  | 排序顺序：asc（升序）或 desc（降序） | 否 | desc |

### 示例

1. 计算山东省bilibili的95值（最近一周）：
```bash
python calculate_95th_percentile.py --province 山东省 --cp bilibili --start-time "2025-03-18 00:00:00" --end-time "2025-03-25 23:59:59" --output 山东省-bilibili.csv
```

2. 只计算接收方向的95值：
```bash
python calculate_95th_percentile.py --province 四川省 --cp 教育网 --start-time "2025-03-01 00:00:00" --end-time "2025-03-26 00:00:00" --direction recv
```

3. 使用自定义配置文件：
```bash
python calculate_95th_percentile.py --province 北京市 --cp 电信 --start-time "2025-03-01 00:00:00" --end-time "2025-03-26 00:00:00" --config my_db_config.ini
```

4. 计算指定院校的95值：
```bash
python calculate_95th_percentile.py --province 四川省 --cp 教育网 --start-time "2025-03-01 00:00:00" --end-time "2025-03-26 00:00:00" --school "电子科技大学,四川大学"
```

5. 导出指定省份和CP在某时间范围内的每日95值：
```bash
python calculate_95th_percentile.py --province 湖北省 --cp 腾讯云 --start-time "2025-04-01 00:00:00" --end-time "2025-04-07 23:59:59" --export-daily --output 湖北省-腾讯云-每日95.csv
```

6. 使用排除名单分别计算“排除院校（逐校）”和“剩余院校（整体汇总）”：
```bash
python calculate_95th_percentile.py \
  --province 四川省 \
  --cp 教育网 \
  --start-time "2025-03-01 00:00:00" \
  --end-time "2025-03-26 00:00:00" \
  --exclude-school "电子科技大学,四川大学" \
  --output 四川省-教育网.csv
```
上述命令会生成两个结果文件：
- `四川省-教育网_excluded.csv`：只包含“电子科技大学、四川大学”的逐校计算结果。
- `四川省-教育网_remaining.csv`：包含“剩余院校汇总”的计算结果（非逐校）。周期模式下一般仅一行；每日模式下为按天多行。

7. 周期模式下按95值从高到低排序输出：
```bash
python calculate_95th_percentile.py --province 山东省 --cp bilibili \
  --start-time "2025-03-18 00:00:00" --end-time "2025-03-25 23:59:59" \
  --sortby 95th_percentile_mbps --sort-order desc
```

8. 每日模式下按“每日95值”从低到高排序输出：
```bash
python calculate_95th_percentile.py --province 湖北省 --cp 腾讯云 \
  --start-time "2025-04-01 00:00:00" --end-time "2025-04-07 23:59:59" \
  --export-daily --sortby daily_95th_percentile_mbps --sort-order asc
```

## 输出结果

工具会将计算结果保存到CSV文件中。输出格式取决于是否使用了 `--export-daily` 参数，以及是否使用了 `--exclude-school` 参数。若指定了 `--sortby`，将在写入前按该字段进行排序，顺序由 `--sort-order` 控制。

**当计算周期汇总95值时 (默认行为):**

CSV文件包含以下字段：
- school_id：院校ID
- ipgroup_name：院校名称（IP组名称）
- ipgroup_id：IP组ID
- nfa_uuid：NFA UUID
- 95th_percentile_mbps：整个周期的95值（Mbps）
- data_points：用于计算该95值的数据点总数
- direction：流量方向

同时，工具会在控制台输出汇总信息，包括：
- 省份
- CP类型
- 时间范围
- 流量方向
- 总院校数
- 平均95值
- 最大95值及对应院校

**当导出每日95值时 (`--export-daily`):**

CSV文件包含以下字段：
- school_id：院校ID
- ipgroup_name：院校名称（IP组名称）
- ipgroup_id：IP组ID
- nfa_uuid：NFA UUID
- date：日期 (YYYY-MM-DD)
- daily_95th_percentile_mbps：当天的95值（Mbps）
- direction：流量方向
- data_points_daily：用于计算当天95值的数据点数量

在此模式下，控制台不会输出汇总统计信息。

**当使用排除名单 (`--exclude-school`) 时：**

- 程序会将查询出来的院校分成两组：
  - 排除组：名称在 `--exclude-school` 列表中的院校（逐校计算）
  - 剩余组：不在上述列表中的其他院校（整体汇总计算：在相同时间点将 recv/send 求和，然后计算95值）
- 会分别计算这两组，并各自输出一个CSV：
  - 在 `--output` 指定的文件名基础上自动添加后缀 `_excluded` 与 `_remaining`
  - 例：`--output result.csv` 将生成 `result_excluded.csv` 与 `result_remaining.csv`
- 剩余组的输出：
  - 周期模式：通常只有一条“剩余院校汇总”的记录
  - 每日模式：按天一条“剩余院校汇总”的记录
- 注意：默认仅统计 `type='yuanxiao'` 的条目，`type='dianbo'`/`'zhibo'` 不会出现在计算结果与名单中。

## 注意事项

1. 时间格式必须为 YYYY-MM-DD HH:MM:SS
2. 确保数据库中有对应的数据表和访问权限
3. 如果未找到符合条件的院校或流速数据，工具会给出相应的警告信息
4. 自 版本更新 起，程序默认过滤 `nfa_ipgroup.type='yuanxiao'`，排除 `dianbo`（点播）与 `zhibo`（直播）
5. 当某院校在所选时间范围内没有任何流量数据点：
   - 周期汇总模式（默认）：该院校将被跳过，不会写入一条 95 值为 0 的记录。
   - 每日导出模式：对没有数据的日期或院校也会跳过写入，对有数据的日期正常计算与输出。
