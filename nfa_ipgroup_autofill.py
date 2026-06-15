#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
按规则自动补全 nfa_ipgroup 表中新增记录的字段：
- check_status 填 0
- type 填 "yuanxiao"
- nfa_name: 仅按 nfa_uuid 的历史记录沿用；若无历史值则留空并提示人工处理
- ipgroup_name: 解析格式 "院校名称_CP名称_IP版本"（兼容 V4/V6 及 V4-1/V6-2，含中英文短横），
  填充 school_name、cp（根据 mapping.json 将显示名映射为简称）
- region: 优先使用命令行参数；否则基于 school_name 的历史记录沿用；否则留空
- school_id: 优先按 school_name 在历史记录中沿用；若无历史则按全表最大纯数字 school_id + 1 分配
- saler_group / saler: 根据 school_name 在历史记录中沿用；否则回退命令行参数；都没有留空

支持 dry-run 预览以及通过 --nfa-uuid 参数限制操作范围（可逗号分隔多个）。

默认对“字段已完整”的记录跳过处理（不做任何写入），仅在提供 --override 时才强制应用规则更新。

默认过滤 is_server=1 的条目（这些条目不在脚本填充范围内）。
"""

import argparse
import pymysql
import sys
import logging
import configparser
import os
import json
import re
from typing import Dict, List, Tuple, Optional, Any

# questionary 用于交互式选择（如果可用）
try:
    import questionary
    QUESTIONARY_AVAILABLE = True
except ImportError:
    QUESTIONARY_AVAILABLE = False
    questionary = None

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("nfa_ipgroup自动补全")

# 仅识别并去除末尾 scale 后缀，例如 [x0.8] / [x0.8x0.8]
_IPGROUP_SCALE_SUFFIX_RE = re.compile(r"\s*\[(?:[xX][0-9]+(?:\.[0-9]+)?)+\]\s*$")

# -------------------- 配置加载 --------------------

def load_db_config(config_file: str) -> Dict:
    if not os.path.exists(config_file):
        logger.error(f"配置文件 {config_file} 不存在")
        create_default_config(config_file)
        logger.info(f"已创建默认配置文件 {config_file}，请修改后重新运行")
        sys.exit(1)
    cfg = configparser.ConfigParser()
    cfg.read(config_file)
    return {
        'host': cfg.get('DATABASE', 'host'),
        'port': cfg.getint('DATABASE', 'port'),
        'user': cfg.get('DATABASE', 'user'),
        'password': cfg.get('DATABASE', 'password'),
        'db': cfg.get('DATABASE', 'db'),
        'charset': cfg.get('DATABASE', 'charset', fallback='utf8mb4')
    }

def create_default_config(config_file: str):
    cfg = configparser.ConfigParser()
    cfg['DATABASE'] = {
        'host': 'localhost',
        'port': '3306',
        'user': 'username',
        'password': 'password',
        'db': 'database',
        'charset': 'utf8mb4'
    }
    with open(config_file, 'w') as f:
        cfg.write(f)

# -------------------- 工具函数 --------------------

def connect_db(db_config: Dict):
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['db'],
            charset=db_config['charset'],
            cursorclass=pymysql.cursors.DictCursor,
        )
        return conn
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        sys.exit(1)


def load_cp_mapping(mapping_file: str) -> Dict[str, str]:
    """加载CP映射：显示名 -> 简称，如 新流 -> xinliu"""
    try:
        with open(mapping_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("mapping 文件格式不正确，应为 JSON 对象")
        logger.info(f"成功加载CP映射，共 {len(data)} 条")
        return data
    except Exception as e:
        logger.error(f"加载CP映射失败: {e}")
        sys.exit(1)


def parse_nfa_uuid_list(nfa_uuid_arg: Optional[str]) -> List[str]:
    if not nfa_uuid_arg:
        return []
    items = [x.strip() for x in nfa_uuid_arg.split(',') if x.strip()]
    return list(dict.fromkeys(items))  # 去重保持顺序


def strip_ipgroup_scale_suffix(value: Optional[str]) -> str:
    """去除名称末尾 scale 后缀（若存在）。"""
    if not value:
        return ''
    return _IPGROUP_SCALE_SUFFIX_RE.sub('', value).strip()


def build_school_name_candidates(school_name: Optional[str]) -> List[str]:
    """为历史查询生成候选 school_name（原值 + 去末尾scale后缀）。"""
    if not school_name:
        return []
    base = school_name.strip()
    if not base:
        return []
    stripped = strip_ipgroup_scale_suffix(base)
    if stripped and stripped != base:
        return [base, stripped]
    return [base]


def parse_ipgroup_name(ipgroup_name: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    解析 ipgroup_name 为 (school_name, cp_display, ip_version)
    预期格式：院校名称_CP显示名_V4/V6，且兼容 V4-1/V6-2（含中英文短横）。
    兼容末尾附带 scale 后缀，如：院校_CP_V4[x0.8x0.8]。
    若无法解析，返回 (None, None, None)
    """
    if not ipgroup_name:
        return None, None, None
    normalized_name = strip_ipgroup_scale_suffix(ipgroup_name)
    parts = (normalized_name or '').split('_')
    if len(parts) < 3:
        return None, None, None
    ip_version_raw = parts[-1].strip()
    ip_version_upper = ip_version_raw.upper()
    # 兼容 V4-1、V6-2、V4—1、V6–2 等：只要以 V4/V6 开头即视为合法，并规范化为 V4/V6
    if ip_version_upper.startswith('V4'):
        ip_version = 'V4'
    elif ip_version_upper.startswith('V6'):
        ip_version = 'V6'
    else:
        return None, None, None
    cp_display = parts[-2].strip()
    school_name = '_'.join(parts[:-2]).strip()
    if not school_name or not cp_display:
        return None, None, None
    return school_name, cp_display, ip_version

# -------------------- 查询辅助 --------------------

def fetch_existing_from_nfa_uuid(cursor, nfa_uuid: str) -> Dict:
    """从 nfa_ipgroup 中按 nfa_uuid 获取已有的非空字段（nfa_name/region等）"""
    cursor.execute(
        """
        SELECT nfa_name, region, type
        FROM nfa_ipgroup
        WHERE nfa_uuid=%s
          AND (nfa_name IS NOT NULL OR region IS NOT NULL)
        ORDER BY (nfa_name IS NULL) ASC, update_time DESC, create_time DESC
        LIMIT 1
        """,
        (nfa_uuid,)
    )
    return cursor.fetchone() or {}


def fetch_school_id_by_name(cursor, school_name: str) -> Optional[str]:
    candidates = build_school_name_candidates(school_name)
    if not candidates:
        return None
    placeholders = ','.join(['%s'] * len(candidates))
    sql = f"""
        SELECT school_id
        FROM nfa_ipgroup
        WHERE school_name IN ({placeholders})
          AND school_id IS NOT NULL
          AND TRIM(school_id) <> ''
        ORDER BY update_time DESC, create_time DESC
        LIMIT 1
    """
    cursor.execute(sql, tuple(candidates))
    row = cursor.fetchone()
    return row['school_id'] if row else None


def fetch_max_numeric_school_id(cursor) -> int:
    """
    获取全表最大纯数字 school_id。
    若存在任意非纯数字的脏数据（排除空字符串），直接报错终止，避免错误分配。
    """
    cursor.execute(
        """
        SELECT school_id
        FROM nfa_ipgroup
        WHERE school_id IS NOT NULL
          AND TRIM(school_id) <> ''
          AND school_id NOT REGEXP '^[0-9]+$'
        LIMIT 1
        """
    )
    bad = cursor.fetchone()
    if bad:
        bad_value = bad.get('school_id')
        raise ValueError(f"检测到非数字 school_id: '{bad_value}'，已停止执行。请先清理数据后重试。")

    cursor.execute(
        """
        SELECT MAX(CAST(school_id AS UNSIGNED)) AS max_school_id
        FROM nfa_ipgroup
        WHERE school_id IS NOT NULL
          AND school_id REGEXP '^[0-9]+$'
        """
    )
    row = cursor.fetchone() or {}
    return int(row.get('max_school_id') or 0)


def fetch_region_by_school(cursor, school_name: str) -> Optional[str]:
    candidates = build_school_name_candidates(school_name)
    if not candidates:
        return None
    placeholders = ','.join(['%s'] * len(candidates))
    sql = f"""
        SELECT region
        FROM nfa_ipgroup
        WHERE school_name IN ({placeholders})
          AND region IS NOT NULL
          AND type='yuanxiao'
          AND (is_server IS NULL OR is_server=0)
        ORDER BY update_time DESC, create_time DESC
        LIMIT 1
    """
    cursor.execute(sql, tuple(candidates))
    row = cursor.fetchone()
    return row['region'] if row else None


def fetch_saler_by_school(cursor, school_name: str) -> Tuple[Optional[str], Optional[str]]:
    candidates = build_school_name_candidates(school_name)
    if not candidates:
        return None, None
    placeholders = ','.join(['%s'] * len(candidates))
    sql = f"""
        SELECT saler_group, saler
        FROM nfa_ipgroup
        WHERE school_name IN ({placeholders})
          AND (saler_group IS NOT NULL OR saler IS NOT NULL)
        ORDER BY update_time DESC, create_time DESC
        LIMIT 1
    """
    cursor.execute(sql, tuple(candidates))
    row = cursor.fetchone() or {}
    return row.get('saler_group'), row.get('saler')


def fetch_pending_records(cursor) -> List[Dict]:
    """查询所有待填充的记录（is_server=0 且关键字段不完整）"""
    cursor.execute(
        """
        SELECT id, ipgroup_id, ipgroup_name, cp, school_name, school_id,
               region, saler_group, saler, nfa_uuid, nfa_name,
               check_status, type, create_time
        FROM nfa_ipgroup
        WHERE (is_server IS NULL OR is_server = 0)
          AND (
            nfa_name IS NULL OR nfa_name = ''
            OR school_name IS NULL OR school_name = ''
            OR cp IS NULL OR cp = ''
            OR region IS NULL OR region = ''
            OR school_id IS NULL OR school_id = ''
            OR saler_group IS NULL OR saler_group = ''
            OR saler IS NULL OR saler = ''
          )
        ORDER BY create_time DESC
        """
    )
    return cursor.fetchall()


def get_missing_fields(row: Dict) -> List[str]:
    """获取该记录缺失的字段列表"""
    missing = []
    for k in ['nfa_name', 'school_name', 'cp', 'region', 'school_id', 'saler_group', 'saler']:
        v = row.get(k)
        if k == 'school_id':
            if v is None or (isinstance(v, str) and v.strip() == ''):
                missing.append(k)
        else:
            if v is None or (isinstance(v, str) and v.strip() == ''):
                missing.append(k)
    return missing


def interactive_select(records: List[Dict]) -> List[int]:
    """
    交互式选择记录
    返回：选中的记录 ID 列表
    """
    if not records:
        return []

    # 如果 questionary 不可用或不在交互式终端，直接使用简单输入方式
    if not QUESTIONARY_AVAILABLE:
        logger.info("questionary 库不可用，使用简单输入方式")
        return simple_select(records)

    # 构建选项列表：显示 id, ipgroup_name, nfa_name, 缺失字段
    choices = []
    for row in records:
        missing = get_missing_fields(row)
        # 截断过长的 ipgroup_name
        ipgroup_name = row.get('ipgroup_name') or ''
        if len(ipgroup_name) > 50:
            ipgroup_name = ipgroup_name[:47] + '...'

        display = f"id={row['id']} | {ipgroup_name}"
        hint = f" (缺：{', '.join(missing[:3])}{'...' if len(missing) > 3 else ''})"
        choices.append(questionary.Choice(title=display + hint, value=row['id']))

    # 多选交互
    try:
        selected_ids = questionary.checkbox(
            "选择需要填充的记录（空格键勾选，a 全选，i 反选，/ 搜索，Enter 确认）:",
            choices=choices,
            instruction="提示：空格键勾选/取消，Enter 确认选择",
            validate=lambda x: len(x) > 0 or "请至少选择一条记录"
        ).ask()
        return selected_ids or []
    except Exception:
        # 回退到简单输入方式
        logger.info("无法使用交互式选择，使用简单输入方式")
        return simple_select(records)


def simple_select(records: List[Dict]) -> List[int]:
    """
    简单输入方式：列出记录，用户输入 ID 列表
    返回：选中的记录 ID 列表
    """
    print("\n请选择需要填充的记录（输入 ID 列表，逗号分隔；输入 'all' 全选）:")
    print("-" * 80)

    # 分组显示（每 10 条一组）
    for i, row in enumerate(records):
        missing = get_missing_fields(row)
        ipgroup_name = row.get('ipgroup_name') or ''
        if len(ipgroup_name) > 40:
            ipgroup_name = ipgroup_name[:37] + '...'

        print(f"  {i+1:3d}. id={row['id']:5d} | {ipgroup_name:40s} | 缺：{', '.join(missing[:4])}")

    print("-" * 80)

    while True:
        user_input = input("请输入选择（如 '1,3,5' 或 'all'）: ").strip().lower()
        if not user_input:
            print("输入为空，请重新输入")
            continue

        if user_input == 'all':
            return [r['id'] for r in records]

        # 解析 ID 列表
        try:
            selected_indices = [int(x.strip()) - 1 for x in user_input.split(',')]
            valid_ids = []
            for idx in selected_indices:
                if 0 <= idx < len(records):
                    valid_ids.append(records[idx]['id'])
                else:
                    print(f"警告：索引 {idx+1} 超出范围 (1-{len(records)})，已忽略")

            if valid_ids:
                return valid_ids
            else:
                print("没有有效的 ID，请重新输入")
        except ValueError:
            print("输入格式错误，请输入逗号分隔的数字（如 '1,3,5'）")


def preview_selections(cursor, records: List[Dict], selected_ids: List[int],
                       cp_mapping: Dict[str, str], args) -> bool:
    """
    预览选中记录的填充内容
    返回：用户是否确认执行
    """
    if not selected_ids:
        return False

    # 构建 id 到 row 的映射
    id_to_row = {r['id']: r for r in records}

    # 获取 school_id 分配器
    try:
        max_school_id = fetch_max_numeric_school_id(cursor)
    except ValueError as e:
        logger.error(str(e))
        return False

    school_id_allocator = {
        'next_school_id': max_school_id + 1 if max_school_id > 0 else 1,
        'school_name_to_id': {}
    }

    # 预览每条记录
    preview_lines = []
    for row_id in selected_ids:
        row = id_to_row.get(row_id)
        if not row:
            continue

        updates, empty_fields = compute_updates_for_row(row, cp_mapping, args)
        apply_historical_overrides(cursor, row, updates, empty_fields, args, school_id_allocator)

        if not updates:
            continue

        preview_lines.append(f"\n  id={row_id} ipgroup_name='{row.get('ipgroup_name')}'")
        for k, v in updates.items():
            old_val = row.get(k)
            preview_lines.append(f"    {k}: '{old_val}' -> '{v}'")

    if not preview_lines:
        logger.info("选中的记录无需更新")
        return False

    # 显示预览
    logger.info("即将填充以下内容：")
    for line in preview_lines:
        print(line)

    # 确认
    if QUESTIONARY_AVAILABLE:
        try:
            confirm = questionary.confirm("确认执行更新？").ask()
            return confirm is True
        except Exception:
            pass

    # 简单输入方式
    while True:
        user_input = input("确认执行更新？(y/n): ").strip().lower()
        if user_input in ('y', 'yes'):
            return True
        elif user_input in ('n', 'no'):
            return False
        else:
            print("请输入 'y' 或 'n'")

# -------------------- 完整性判断 --------------------

def _is_nonempty(value) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ''
    return True

def is_row_complete(row: Dict) -> bool:
    """判断该条记录关键字段是否已完整。
    关键字段：nfa_name, school_name, cp, region, school_id, saler_group, saler。
    注：type/check_status 不纳入“完整性”判断，避免对已成型数据做无谓改动。
    """
    required_keys = ['nfa_name', 'school_name', 'cp', 'region', 'school_id', 'saler_group', 'saler']
    for k in required_keys:
        v = row.get(k)
        if k == 'school_id':
            if v is None:
                return False
        else:
            if not _is_nonempty(v):
                return False
    return True

# -------------------- 主处理逻辑 --------------------

def build_select_sql(nfa_uuid_list: List[str]) -> Tuple[str, Tuple]:
    base = (
        "SELECT id, ipgroup_id, ipgroup_name, cp, school_name, school_id, region, "
        "saler_group, saler, nfa_uuid, nfa_name, check_status, type, create_time, update_time "
        "FROM nfa_ipgroup"
    )
    params: List = []
    server_filter = "(is_server IS NULL OR is_server=0)"
    if nfa_uuid_list:
        placeholders = ','.join(['%s'] * len(nfa_uuid_list))
        where = f" WHERE nfa_uuid IN ({placeholders}) AND {server_filter}"
        return base + where, tuple(nfa_uuid_list)
    else:
        where = f" WHERE {server_filter}"
        return base + where, tuple()


def compute_updates_for_row(row: Dict,
                             cp_mapping: Dict[str, str],
                             args) -> Tuple[Dict[str, object], List[str]]:
    """
    根据规则计算该行需要更新的字段和值。
    返回：
      updates: 将要写入DB的字段字典（仅包含需要更新的字段）
      empty_fields: 最终依然为空（无法填充）的字段名列表
    """
    updates: Dict[str, object] = {}
    empty_fields: List[str] = []

    # 统一：check_status=0，type='yuanxiao'
    if row.get('check_status') != 0:
        updates['check_status'] = 0
    if row.get('type') != 'yuanxiao':
        updates['type'] = 'yuanxiao'

    # 解析 ipgroup_name -> school_name, cp
    parsed_school_name, cp_display, ip_version = parse_ipgroup_name(row.get('ipgroup_name'))

    # school_name
    if not row.get('school_name'):
        if parsed_school_name:
            updates['school_name'] = parsed_school_name
        else:
            empty_fields.append('school_name')  # 无法从 ipgroup_name 解析

    # cp 映射（显示名 -> 简称）
    if not row.get('cp'):
        if cp_display:
            cp_display_clean = strip_ipgroup_scale_suffix(cp_display)
            mapped_cp = cp_mapping.get(cp_display_clean) or cp_mapping.get(cp_display)
            if mapped_cp:
                updates['cp'] = mapped_cp
            else:
                empty_fields.append('cp')  # 无法映射
        else:
            empty_fields.append('cp')  # 无法从 ipgroup_name 解析

    # region：优先命令行，其次历史
    if not row.get('region'):
        if args.region:
            updates['region'] = args.region
            if getattr(args, 'trace_source', False):
                logger.info(f"region来源[CLI参数] id={row.get('id')} -> '{args.region}'")
        else:
            # 历史沿用
            # 在 compute_updates_for_row 之外无法直接访问 cursor，这里放到调用处补齐；
            # 为保持单函数职责，这里先做占位，由调用者完成。
            pass

    # nfa_name：仅按 nfa_uuid 历史；若无历史值则留空
    if not row.get('nfa_name'):
        # 留空占位，调用方若查不到历史值会把字段记录为空
        empty_fields.append('nfa_name')

    # school_id：根据 school_name 历史沿用
    if not row.get('school_id') and (row.get('school_name') or parsed_school_name):
        # 实际值由调用方查询并回填
        pass

    # saler_group / saler：先按 school_name 历史沿用，否则回退参数
    if not row.get('saler_group'):
        if args.saler_group:
            updates['saler_group'] = args.saler_group
        else:
            empty_fields.append('saler_group')
    if not row.get('saler'):
        if args.saler:
            updates['saler'] = args.saler
        else:
            empty_fields.append('saler')

    return updates, empty_fields


def apply_historical_overrides(cursor,
                               row: Dict,
                               updates: Dict[str, object],
                               empty_fields: List[str],
                               args,
                               school_id_allocator: Dict[str, Any]):
    """根据历史记录（nfa_uuid、school_name）进一步完善 region / nfa_name / school_id / saler*"""
    nfa_uuid = row.get('nfa_uuid')
    # 优先使用从 ipgroup_name 解析出的 school_name 作为后续 region 查找依据
    parsed_school_name, _, _ = parse_ipgroup_name(row.get('ipgroup_name'))
    school_name = updates.get('school_name') or parsed_school_name or row.get('school_name')

    # nfa_uuid 历史：nfa_name（region 不再使用 nfa_uuid）
    if nfa_uuid:
        hist = fetch_existing_from_nfa_uuid(cursor, nfa_uuid)
        # 若当前 nfa_name 为空且历史有值，则优先用历史覆盖
        # region 不再从 nfa_uuid 沿用，改为后续统一按 school 历史查找
        # nfa_name：若当前为空且历史有值，则优先用历史覆盖（高于命令行参数）
        if (not row.get('nfa_name')) and hist.get('nfa_name'):
            updates['nfa_name'] = hist['nfa_name']
            if getattr(args, 'trace_source', False):
                logger.info(f"nfa_name来源[nfa_uuid历史] id={row.get('id')} nfa_uuid={nfa_uuid} -> '{hist['nfa_name']}'")
            if 'nfa_name' in empty_fields:
                try:
                    empty_fields.remove('nfa_name')
                except ValueError:
                    pass

    # region：若当前仍为空且未指定命令行参数，则基于 school_name 的历史沿用
    if (not row.get('region')) and ('region' not in updates) and school_name and not args.region:
        r = fetch_region_by_school(cursor, school_name)
        if r:
            updates['region'] = r
            if getattr(args, 'trace_source', False):
                logger.info(f"region来源[school历史] id={row.get('id')} school='{school_name}' -> '{r}'")
            if 'region' in empty_fields:
                try:
                    empty_fields.remove('region')
                except ValueError:
                    pass

    # 不再进行 school/cp/region 维度的猜测，nfa_name 仅按 nfa_uuid 历史沿用

    # school_id：先按 school_name 历史沿用；若无历史则按全表最大纯数字+1分配
    if (not row.get('school_id')) and school_name:
        sid = fetch_school_id_by_name(cursor, school_name)
        if sid is not None:
            updates['school_id'] = str(sid)
            if getattr(args, 'trace_source', False):
                logger.info(f"school_id来源[school历史] id={row.get('id')} school='{school_name}' -> '{sid}'")
        else:
            school_name_to_id = school_id_allocator['school_name_to_id']
            if school_name in school_name_to_id:
                new_sid = school_name_to_id[school_name]
                updates['school_id'] = new_sid
                if getattr(args, 'trace_source', False):
                    logger.info(f"school_id来源[本次新院校复用] id={row.get('id')} school='{school_name}' -> '{new_sid}'")
            else:
                next_sid = int(school_id_allocator['next_school_id'])
                new_sid = str(next_sid)
                school_name_to_id[school_name] = new_sid
                school_id_allocator['next_school_id'] = next_sid + 1
                updates['school_id'] = new_sid
                if getattr(args, 'trace_source', False):
                    logger.info(f"school_id来源[本次新院校分配] id={row.get('id')} school='{school_name}' -> '{new_sid}'")

            if 'school_id' in empty_fields:
                try:
                    empty_fields.remove('school_id')
                except ValueError:
                    pass

    # saler_group / saler：若仍为空且有 school_name，优先用历史；否则回退命令行参数
    if school_name:
        sg_hist, s_hist = fetch_saler_by_school(cursor, school_name)
        # saler_group 先历史，后参数
        if not row.get('saler_group'):
            if sg_hist:
                updates['saler_group'] = sg_hist
                if 'saler_group' in empty_fields:
                    try:
                        empty_fields.remove('saler_group')
                    except ValueError:
                        pass
            elif ('saler_group' not in updates or not updates.get('saler_group')) and args.saler_group:
                updates['saler_group'] = args.saler_group
                if 'saler_group' in empty_fields:
                    try:
                        empty_fields.remove('saler_group')
                    except ValueError:
                        pass
        # saler 先历史，后参数
        if not row.get('saler'):
            if s_hist:
                updates['saler'] = s_hist
                if 'saler' in empty_fields:
                    try:
                        empty_fields.remove('saler')
                    except ValueError:
                        pass
            elif ('saler' not in updates or not updates.get('saler')) and args.saler:
                updates['saler'] = args.saler
                if 'saler' in empty_fields:
                    try:
                        empty_fields.remove('saler')
                    except ValueError:
                        pass


def build_update_sql_and_params(updates: Dict[str, object], row_id: int) -> Tuple[str, Tuple]:
    keys = list(updates.keys())
    if not keys:
        return "", tuple()
    set_clause = ", ".join([f"{k}=%s" for k in keys])
    sql = f"UPDATE nfa_ipgroup SET {set_clause} WHERE id=%s"
    params = tuple(updates[k] for k in keys) + (row_id,)
    return sql, params

# -------------------- 运行入口 --------------------

def run(args):
    db_cfg = load_db_config(args.config)
    conn = connect_db(db_cfg)
    cursor = conn.cursor()
    cp_mapping = load_cp_mapping(args.mapping)

    # 交互模式：自动发现待填充记录供用户选择
    if getattr(args, 'interactive', False):
        logger.info("正在查询待填充记录...")
        pending_records = fetch_pending_records(cursor)
        if not pending_records:
            logger.info("没有需要填充的记录")
            cursor.close()
            conn.close()
            return

        logger.info(f"共发现 {len(pending_records)} 条待填充记录")

        # 交互式选择
        selected_ids = interactive_select(pending_records)
        if not selected_ids:
            logger.info("未选择任何记录")
            cursor.close()
            conn.close()
            return

        # 预览填充内容
        if not preview_selections(cursor, pending_records, selected_ids, cp_mapping, args):
            logger.info("已取消操作")
            cursor.close()
            conn.close()
            return

        # 交互模式下，后续查询限制在选中的 id 范围内
        selected_id_set = set(selected_ids)
    else:
        selected_id_set = None

    try:
        max_school_id = fetch_max_numeric_school_id(cursor)
    except ValueError as e:
        logger.error(str(e))
        cursor.close()
        conn.close()
        sys.exit(1)

    school_id_allocator: Dict[str, Any] = {
        'next_school_id': max_school_id + 1 if max_school_id > 0 else 1,
        'school_name_to_id': {}
    }
    logger.info(
        f"school_id分配器初始化完成：当前最大纯数字school_id={max_school_id}，"
        f"新院校将从 {school_id_allocator['next_school_id']} 开始分配"
    )

    # 读取目标记录
    nfa_uuid_list = parse_nfa_uuid_list(args.nfa_uuid)
    select_sql, select_params = build_select_sql(nfa_uuid_list)

    # 交互模式下限制在选中的 id 范围内
    if selected_id_set is not None:
        # 追加 AND id IN (...) 到 WHERE 子句后面
        select_sql = f"{select_sql} AND id IN ({','.join(['%s'] * len(selected_id_set))})"
        select_params = select_params + tuple(selected_id_set)

    cursor.execute(select_sql, select_params)
    rows = cursor.fetchall()
    logger.info(f"共加载 {len(rows)} 条记录用于处理")

    total_updates = 0
    preview_changes: List[Dict] = []
    empties_summary: List[Tuple[int, str, List[str]]] = []  # (id, ipgroup_name, [fields])

    for row in rows:
        # 若未开启 override，则对字段完整的条目直接跳过
        if not args.override and is_row_complete(row):
            if getattr(args, 'trace_source', False):
                logger.info(f"跳过(完整) id={row['id']} ipgroup='{row.get('ipgroup_name')}' nfa_name='{row.get('nfa_name')}'")
            else:
                logger.info(f"跳过 id={row['id']} ipgroup='{row.get('ipgroup_name')}'（关键字段已完整）")
            continue
        updates, empty_fields = compute_updates_for_row(row, cp_mapping, args)
        # 用历史数据进行二次填充
        apply_historical_overrides(cursor, row, updates, empty_fields, args, school_id_allocator)

        # 若 region 仍为空，记录空项
        if (not row.get('region')) and ('region' not in updates):
            if 'region' not in empty_fields:
                empty_fields.append('region')
        # 若 nfa_name 仍未填（既无历史也无参数）
        if (not row.get('nfa_name')) and ('nfa_name' not in updates):
            if 'nfa_name' not in empty_fields:
                empty_fields.append('nfa_name')

        if not updates:
            # 无需更新也要输出空项提示
            if empty_fields:
                empties_summary.append((row['id'], row.get('ipgroup_name'), empty_fields.copy()))
            continue

        # 预览内容
        preview = {
            'id': row['id'],
            'ipgroup_id': row.get('ipgroup_id'),
            'ipgroup_name': row.get('ipgroup_name'),
            'changes': {},
        }
        for k, v in updates.items():
            preview['changes'][k] = {
                'old': row.get(k),
                'new': v,
            }
        preview_changes.append(preview)

        # 记录空项字段
        if empty_fields:
            empties_summary.append((row['id'], row.get('ipgroup_name'), empty_fields.copy()))

        # 执行更新
        if args.execute:
            sql, params = build_update_sql_and_params(updates, row['id'])
            if sql:
                try:
                    cursor.execute(sql, params)
                    total_updates += 1
                    if total_updates % 500 == 0:
                        conn.commit()
                        logger.info(f"已提交 {total_updates} 条更新")
                except Exception as e:
                    logger.error(f"更新 id={row['id']} 失败: {e}")
        else:
            # dry-run 日志
            change_items = [f"{k}: '{row.get(k)}' -> '{v}'" for k, v in updates.items()]
            logger.info(f"[dry-run] id={row['id']} ipgroup='{row.get('ipgroup_name')}' 将更新: " + "; ".join(change_items))
            if getattr(args, 'trace_source', False) and 'nfa_name' in updates:
                logger.info(f"    ↳ nfa_name来源: nfa_uuid历史")

    # 提交并收尾
    if args.execute and total_updates > 0:
        conn.commit()
        logger.info(f"实际更新完成，共 {total_updates} 条记录")
    else:
        logger.info(f"预览完成，共 {len(preview_changes)} 条记录需要更新")

    # 输出置空字段摘要
    if empties_summary:
        logger.warning("以下记录存在仍未能自动填充的字段，请人工后续处理：")
        for rid, ipgname, fields in empties_summary:
            logger.warning(f"  id={rid} ipgroup_name='{ipgname}' 空字段: {', '.join(sorted(set(fields))) }")
    else:
        logger.info("所有目标字段均已自动填充，无需人工补齐。")

    cursor.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description='nfa_ipgroup 字段自动补全工具')
    parser.add_argument('--config', default='db_config.ini', help='数据库配置文件路径')
    parser.add_argument('--mapping', default='mapping.json', help='CP映射文件路径（显示名->简称）')
    parser.add_argument('--nfa-uuid', dest='nfa_uuid', default=None, help='限制操作范围的 nfa_uuid，可逗号分隔多个')
    parser.add_argument('--region', default=None, help='用于填充 region 的参数。若未提供则尝试沿用同 nfa_uuid 的历史记录')
    # 移除 --nfa-name 回退参数，nfa_name 仅按 nfa_uuid 历史沿用
    parser.add_argument('--saler-group', dest='saler_group', default=None, help='用于回退填充 saler_group 的参数（历史不存在时使用）')
    parser.add_argument('--saler', dest='saler', default=None, help='用于回退填充 saler 的参数（历史不存在时使用）')
    parser.add_argument('--override', action='store_true', help='当记录关键字段已完整时，仍强制应用规则进行更新')
    parser.add_argument('--trace-source', dest='trace_source', action='store_true', help='输出字段填充来源的调试信息（便于排查为何未命中历史）')
    parser.add_argument('--execute', action='store_true', help='实际执行更新。不加此参数则为 dry-run 预览')
    parser.add_argument('--interactive', '-i', dest='interactive', action='store_true', help='交互模式：自动发现待填充记录供选择，无需手动指定 --nfa-uuid')

    args = parser.parse_args()
    run(args)


if __name__ == '__main__':
    main()
