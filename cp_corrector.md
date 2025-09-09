# CP字段校正工具使用说明

这个工具用于检查并纠正数据表中的CP字段。根据`ipgroup_name`字段中的y部分和提供的映射关系，检查当前cp字段是否符合预期，如果不符合则进行修改。

## 功能特点

- 解析`ipgroup_name`字段，提取其中的y部分（格式为x_y_z中的y）
- 根据提供的y到cp的映射关系，检查当前cp字段是否符合预期
- 对不符合预期的记录进行修正
- 支持演习模式（不实际更新数据库）
- 支持导出需要纠正的记录到文件

## 安装依赖

在使用前，请确保已安装所需的依赖：

```bash
pip install mysql-connector-python
```

## 使用方法

```bash
python cp_corrector.py --host 数据库主机 --user 用户名 --password 密码 --database 数据库名 --mapping 映射文件路径 [--nfa-uuid NFA_UUID] [--export 导出文件] [--execute]
```

### 参数说明

- `--host`: 数据库主机地址，默认为localhost
- `--port`: 数据库端口，默认为3306
- `--user`: 数据库用户名（必填）
- `--password`: 数据库密码（必填）
- `--database`: 数据库名（必填）
- `--mapping`: y到cp的映射文件路径（必填）
- `--nfa-uuid`: 可选，用于约束影响范围的nfa_uuid，只处理指定nfa_uuid的记录
- `--export`: 可选，将需要纠正的记录导出到指定文件
- `--execute`: 可选，实际执行更新操作，不加此参数则为演习模式（只显示将要更新的内容，不实际更新数据库）

## 映射文件格式

工具支持三种格式的映射文件：

1. **JSON格式**：
```json
{
  "y值1": "cp值1",
  "y值2": "cp值2",
  "y值3": "cp值3"
}
```

2. **CSV格式**：第一行为表头，之后每行为y值和对应的cp值
```
y值,cp值
白山,bsy
腾讯,txwy
阿里,aliyun
```

3. **文本格式**：每行一个映射关系，用逗号分隔y值和cp值
```
白山,bsy
腾讯,txwy
阿里,aliyun
```

## 使用示例

### 演习模式（不实际更新数据库）

```bash
python cp_corrector.py --host localhost --user root --password 123456 --database mydb --mapping mapping.json
```

### 导出需要纠正的记录

```bash
python cp_corrector.py --host localhost --user root --password 123456 --database mydb --mapping mapping.json --export corrections.csv
```

### 约束影响范围

```bash
python cp_corrector.py --host localhost --user root --password 123456 --database mydb --mapping mapping.json --nfa-uuid abc123 --execute
```

### 实际执行更新

```bash
python cp_corrector.py --host localhost --user root --password 123456 --database mydb --mapping mapping.json --execute
```

## 输出示例

演习模式下的输出示例：
```
成功连接到数据库 mydb
成功加载映射关系，共 4 条
正在查询所有记录...
共获取 246945297 条记录
发现 1523 条记录需要纠正
[演习模式] 将更新记录 ID: 12345, ipgroup_name: abc_白山_xyz, CP从 'ws' 更新为 'bsy'
[演习模式] 将更新记录 ID: 12346, ipgroup_name: def_B站_uvw, CP从 'bili' 更新为 'bilibili'
...
[演习模式] 共有 1523 条记录需要更新
数据库连接已关闭
```

使用nfa_uuid参数约束影响范围的输出示例：
```
成功连接到数据库 mydb
成功加载映射关系，共 4 条
正在查询nfa_uuid为 abc123 的记录...
共获取 1245 条记录
发现 37 条记录需要纠正
[演习模式] 将更新记录 ID: 12345, ipgroup_name: abc_白山_xyz, CP从 'ws' 更新为 'bsy'
...
[演习模式] 共有 37 条记录需要更新
数据库连接已关闭
```

## 注意事项

1. 默认情况下，工具处于演习模式，不会实际修改数据库。如需实际更新，请添加`--execute`参数。
2. 建议先使用`--export`参数导出需要修改的记录，确认无误后再使用`--execute`参数执行实际更新。
3. 工具会每500条记录提交一次事务，以避免事务过大。
4. 如果映射文件格式不正确，工具会报错并退出。
5. 强烈建议使用`--nfa-uuid`参数来约束影响范围，特别是在生产环境中执行更新操作时。
