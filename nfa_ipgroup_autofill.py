#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
按规则自动补全 nfa_ipgroup 表中新增记录的字段：
- check_status 填 0
- type 填 "yuanxiao"
- nfa_name: 优先同 nfa_uuid 的历史记录沿用；否则回退命令行参数；都没有留空
- ipgroup_name: 解析格式 "院校名称_CP名称_IP版本"（兼容 V4/V6 及 V4-1/V6-2，含中英文短横），
  填充 school_name、cp（根据 mapping.json 将显示名映射为简称）
- region: 优先使用命令行参数；否则同 nfa_uuid 的历史记录沿用；否则留空
- school_id: 根据 school_name 在历史记录中沿用（取最近的非空值）
- saler_group / saler: 根据 school_name 在历史记录中沿用；否则回退命令行参数；都没有留空

支持 dry-run 预览以及通过 --nfa-uuid 参数限制操作范围（可逗号分隔多个）。
"""

import argparse
import pymysql
import sys
import logging
import configparser
import os
import json
from typing import Dict, List, Tuple, Optional

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("nfa_ipgroup自动补全")

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


def parse_ipgroup_name(ipgroup_name: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    解析 ipgroup_name 为 (school_name, cp_display, ip_version)
    预期格式：院校名称_CP显示名_V4/V6，且兼容 V4-1/V6-2（含中英文短横）。
    若无法解析，返回 (None, None, None)
    """
    if not ipgroup_name:
        return None, None, None
    parts = (ipgroup_name or '').split('_')
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
          AND (nfa_name IS NOT NULL OR region IS NOT NULL OR type IS NOT NULL)
        ORDER BY update_time DESC, create_time DESC
        LIMIT 1
        """,
        (nfa_uuid,)
    )
    return cursor.fetchone() or {}


def fetch_school_id_by_name(cursor, school_name: str) -> Optional[int]:
    cursor.execute(
        """
        SELECT school_id
        FROM nfa_ipgroup
        WHERE school_name=%s AND school_id IS NOT NULL
        ORDER BY update_time DESC, create_time DESC
        LIMIT 1
        """,
        (school_name,)
    )
    row = cursor.fetchone()
    return row['school_id'] if row else None


def fetch_saler_by_school(cursor, school_name: str) -> Tuple[Optional[str], Optional[str]]:
    cursor.execute(
        """
        SELECT saler_group, saler
        FROM nfa_ipgroup
        WHERE school_name=%s AND (saler_group IS NOT NULL OR saler IS NOT NULL)
        ORDER BY update_time DESC, create_time DESC
        LIMIT 1
        """,
        (school_name,)
    )
    row = cursor.fetchone() or {}
    return row.get('saler_group'), row.get('saler')

# -------------------- 主处理逻辑 --------------------

def build_select_sql(nfa_uuid_list: List[str]) -> Tuple[str, Tuple]:
    base = (
        "SELECT id, ipgroup_id, ipgroup_name, cp, school_name, school_id, region, "
        "saler_group, saler, nfa_uuid, nfa_name, check_status, type, create_time, update_time "
        "FROM nfa_ipgroup"
    )
    params: List = []
    if nfa_uuid_list:
        placeholders = ','.join(['%s'] * len(nfa_uuid_list))
        where = f" WHERE nfa_uuid IN ({placeholders})"
        return base + where, tuple(nfa_uuid_list)
    else:
        return base, tuple()


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
            mapped_cp = cp_mapping.get(cp_display)
            if mapped_cp:
                updates['cp'] = mapped_cp
            else:
                empty_fields.append('cp')  # 无法映射
        else:
            empty_fields.append('cp')  # 无法从 ipgroup_name 解析

    # region：优先命令行，其次历史同 nfa_uuid
    if not row.get('region'):
        if args.region:
            updates['region'] = args.region
        else:
            # 历史沿用
            # 在 compute_updates_for_row 之外无法直接访问 cursor，这里放到调用处补齐；
            # 为保持单函数职责，这里先做占位，由调用者完成。
            pass

    # nfa_name：优先 nfa_uuid 历史，其次命令行
    if not row.get('nfa_name'):
        if args.nfa_name:
            # 临时先用参数，若稍后能查到历史值，调用方会覆盖
            updates['nfa_name'] = args.nfa_name
        else:
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


def apply_historical_overrides(cursor, row: Dict, updates: Dict[str, object], empty_fields: List[str], args):
    """根据历史记录（nfa_uuid、school_name）进一步完善 region / nfa_name / school_id / saler*"""
    nfa_uuid = row.get('nfa_uuid')
    school_name = updates.get('school_name') or row.get('school_name')

    # nfa_uuid 历史：region / nfa_name
    if nfa_uuid:
        hist = fetch_existing_from_nfa_uuid(cursor, nfa_uuid)
        # region：若当前仍为空，且命令行未指定，则用历史
        if (not row.get('region')) and ('region' not in updates) and hist.get('region') and not args.region:
            updates['region'] = hist['region']
            if 'region' in empty_fields:
                try:
                    empty_fields.remove('region')
                except ValueError:
                    pass
        # nfa_name：若当前为空且历史有值，则优先用历史覆盖（高于命令行参数）
        if (not row.get('nfa_name')) and hist.get('nfa_name'):
            updates['nfa_name'] = hist['nfa_name']
            if 'nfa_name' in empty_fields:
                try:
                    empty_fields.remove('nfa_name')
                except ValueError:
                    pass

    # school_id：按 school_name 历史沿用
    if (not row.get('school_id')) and school_name:
        sid = fetch_school_id_by_name(cursor, school_name)
        if sid is not None:
            updates['school_id'] = sid
        else:
            if 'school_id' not in empty_fields:
                empty_fields.append('school_id')

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

    # 读取目标记录
    nfa_uuid_list = parse_nfa_uuid_list(args.nfa_uuid)
    select_sql, select_params = build_select_sql(nfa_uuid_list)
    cursor.execute(select_sql, select_params)
    rows = cursor.fetchall()
    logger.info(f"共加载 {len(rows)} 条记录用于处理")

    total_updates = 0
    preview_changes: List[Dict] = []
    empties_summary: List[Tuple[int, str, List[str]]] = []  # (id, ipgroup_name, [fields])

    for row in rows:
        updates, empty_fields = compute_updates_for_row(row, cp_mapping, args)
        # 用历史数据进行二次填充
        apply_historical_overrides(cursor, row, updates, empty_fields, args)

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
    parser.add_argument('--nfa-name', dest='nfa_name', default=None, help='用于回退填充 nfa_name 的参数（历史不存在时使用）')
    parser.add_argument('--saler-group', dest='saler_group', default=None, help='用于回退填充 saler_group 的参数（历史不存在时使用）')
    parser.add_argument('--saler', dest='saler', default=None, help='用于回退填充 saler 的参数（历史不存在时使用）')
    parser.add_argument('--execute', action='store_true', help='实际执行更新。不加此参数则为 dry-run 预览')

    args = parser.parse_args()
    run(args)


if __name__ == '__main__':
    main()
