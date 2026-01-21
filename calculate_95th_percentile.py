#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
计算指定省份、指定CP类型、指定时间范围内所有院校的95值工具
"""

import argparse
import pymysql
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import configparser
import os
import sys
import logging
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("95值计算工具")

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='计算指定省份、指定CP类型、指定时间范围或年度每月的95值')
    parser.add_argument('--province', '-p', required=True, help='指定省份，例如：四川')
    parser.add_argument('--cp', '-c', required=False, help='指定CP类型，例如：教育网；如不提供且指定 --year，则对该省份所有CP分别计算')
    parser.add_argument('--year', type=int, help='指定年份，启用按月计算该年度每个月的95值')
    parser.add_argument('--start-time', '-s', required=False, help='开始时间，格式：YYYY-MM-DD HH:MM:SS（在未指定 --year 时必需）')
    parser.add_argument('--end-time', '-e', required=False, help='结束时间，格式：YYYY-MM-DD HH:MM:SS（在未指定 --year 时必需）')
    parser.add_argument('--config', default='db_config.ini', help='数据库配置文件路径（若未提供 --config-rel/--config-data 时使用）')
    parser.add_argument('--config-rel', default=None, help='关系库配置文件（nfa_ipgroup 等）')
    parser.add_argument('--config-data', default=None, help='流量库配置文件（nfa_ip_group_speed_logs_5m 等）')
    parser.add_argument('--output', '-o', default='95th_percentile_results.csv', help='输出结果文件路径')
    parser.add_argument('--direction', '-d', default='both', choices=['send', 'recv', 'both'], 
                        help='流量方向：send(发送)、recv(接收)或both(双向)')
    parser.add_argument('--school', '-sc', help='指定院校名称，多个院校用逗号分隔，例如：电子科技大学,四川大学')
    parser.add_argument('--export-daily', action='store_true', help='导出每日95值，而不是整个周期的汇总95值（仅非年度模式）')
    parser.add_argument('--exclude-school', '-esc', help='排除的院校名称，多个院校用逗号分隔，例如：电子科技大学,四川大学')
    parser.add_argument('--sortby', help='按该字段排序输出，例如：95th_percentile_mbps、daily_95th_percentile_mbps、ipgroup_name 等')
    parser.add_argument('--sort-order', choices=['asc', 'desc'], default='desc', help='排序顺序：asc（升序）或 desc（降序），默认 desc')
    parser.add_argument('--aggregate-all', action='store_true',
                        help='将所有符合条件的院校在相同时间点上汇总（recv/send 求和）后再计算95值；配合 --export-daily 则输出“全市汇总”的日95。')
    parser.add_argument('--batch-size', type=int, default=200,
                        help='批量拉取日志时每批包含的 (ipgroup_id, nfa_uuid) 数量，默认 200')
    parser.add_argument('--group-by-school', action='store_true',
                        help='按 region+cp+school_name 聚合（跨 ipgroup 历史实例累加）后计算95值')
    parser.add_argument('--jobs', type=int, default=1, help='年度模式并行处理月份的线程数，默认 1（不并行）')
    return parser.parse_args()

def load_db_config(config_file, section='DATABASE'):
    """加载数据库配置"""
    if not os.path.exists(config_file):
        logger.error(f"配置文件 {config_file} 不存在")
        create_default_config(config_file)
        logger.info(f"已创建默认配置文件 {config_file}，请修改后重新运行")
        sys.exit(1)
    
    config = configparser.ConfigParser()
    config.read(config_file)
    return {
        'host': config.get(section, 'host'),
        'port': config.getint(section, 'port'),
        'user': config.get(section, 'user'),
        'password': config.get(section, 'password'),
        'db': config.get(section, 'db'),
        'charset': config.get(section, 'charset', fallback='utf8mb4')
    }

def create_default_config(config_file):
    """创建默认配置文件"""
    config = configparser.ConfigParser()
    config['DATABASE'] = {
        'host': 'localhost',
        'port': '3306',
        'user': 'username',
        'password': 'password',
        'db': 'database',
        'charset': 'utf8mb4'
    }
    with open(config_file, 'w') as f:
        config.write(f)

def connect_to_db(db_config):
    """连接到数据库"""
    try:
        connection = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            db=db_config['db'],
            charset=db_config['charset'],
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except Exception as e:
        logger.error(f"数据库连接失败: {e}")
        sys.exit(1)

def _pick_section(config: configparser.ConfigParser, preferred_sections):
    # 返回第一个存在的节；若都不存在且仅有一个节，则返回该节；否则返回 None
    if preferred_sections:
        for s in preferred_sections:
            if config.has_section(s):
                return s
    sections = config.sections()
    if len(sections) == 1:
        return sections[0]
    return None

def load_db_config_flexible(config_file: str, preferred_sections=()):
    if not os.path.exists(config_file):
        logger.error(f"配置文件 {config_file} 不存在")
        create_default_config(config_file)
        logger.info(f"已创建默认配置文件 {config_file}，请修改后重新运行")
        sys.exit(1)
    cfg = configparser.ConfigParser()
    cfg.read(config_file)
    section = _pick_section(cfg, preferred_sections or ('DATABASE',))
    if not section:
        logger.error(f"配置文件 {config_file} 未找到可用节名，请在其中添加 [DATABASE] 或 [REL_DATABASE]/[DATA_DATABASE]。现有节: {cfg.sections()}")
        sys.exit(1)
    return {
        'host': cfg.get(section, 'host'),
        'port': cfg.getint(section, 'port'),
        'user': cfg.get(section, 'user'),
        'password': cfg.get(section, 'password'),
        'db': cfg.get(section, 'db'),
        'charset': cfg.get(section, 'charset', fallback='utf8mb4')
    }

def _load_dual_db_configs(args):
    cfg_rel = None
    cfg_data = None
    if args.config_rel:
        cfg_rel = load_db_config_flexible(args.config_rel, ('REL_DATABASE', 'DATABASE'))
    if args.config_data:
        cfg_data = load_db_config_flexible(args.config_data, ('DATA_DATABASE', 'DATABASE'))
    if cfg_rel and cfg_data:
        return cfg_rel, cfg_data
    # 在单个 ini 中尝试独立段
    base_ini = args.config or 'db_config.ini'
    if os.path.exists(base_ini):
        cp = configparser.ConfigParser()
        cp.read(base_ini)
        if cp.has_section('REL_DATABASE'):
            cfg_rel = load_db_config(base_ini, section='REL_DATABASE')
        if cp.has_section('DATA_DATABASE'):
            cfg_data = load_db_config(base_ini, section='DATA_DATABASE')
    # 回退到单一 DATABASE 段
    if not cfg_rel:
        cfg_rel = load_db_config_flexible(base_ini, ('REL_DATABASE', 'DATABASE'))
    if not cfg_data:
        cfg_data = load_db_config_flexible(base_ini, ('DATA_DATABASE', 'DATABASE'))
    return cfg_rel, cfg_data

def get_schools_by_province_and_cp(connection, province, cp, school_names_str=None):
    """获取指定省份、CP类型以及可选的指定院校的所有院校（仅 type='yuanxiao'）"""
    base_query = """
    SELECT DISTINCT school_id, school_name, ipgroup_name, ipgroup_id, nfa_uuid, cp, region
    FROM nfa_ipgroup
    WHERE region = %s AND type = %s
    """
    params = [province, 'yuanxiao']
    if cp:
        base_query += " AND cp = %s"
        params.append(cp)

    if school_names_str:
        school_names_list = [name.strip() for name in school_names_str.split(',') if name.strip()]
        if school_names_list:
            placeholders = ', '.join(['%s'] * len(school_names_list))
            base_query += f" AND school_name IN ({placeholders})"
            params.extend(school_names_list)
            logger.info(f"筛选条件：省份='{province}', CP='{cp or 'ALL'}', type='yuanxiao', 指定院校='{school_names_str}'")
        else:
            logger.warning("提供的 --school 参数值为空或格式不正确，将忽略院校名称筛选。")
            logger.info(f"筛选条件：省份='{province}', CP='{cp or 'ALL'}', type='yuanxiao' (未指定有效院校)")
    else:
        logger.info(f"筛选条件：省份='{province}', CP='{cp or 'ALL'}', type='yuanxiao' (未指定院校)")

    query = base_query
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, tuple(params))
            schools = cursor.fetchall()
            logger.info(f"查询完毕，共找到 {len(schools)} 所符合条件的院校 (type='yuanxiao')")
            return schools
    except Exception as e:
        logger.error(f"查询院校信息失败: {e}")
        return []

def get_speed_data(connection, ipgroup_id, nfa_uuid, start_time, end_time):
    """获取指定IP组和时间范围的流速数据"""
    query = """
    SELECT create_time, recv, send
    FROM nfa_ip_group_speed_logs_5m
    WHERE ipgroup_id = %s AND nfa_uuid = %s AND create_time BETWEEN %s AND %s
    ORDER BY create_time
    """
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (ipgroup_id, nfa_uuid, start_time, end_time))
            data = cursor.fetchall()
            return data
    except Exception as e:
        logger.error(f"查询流速数据失败 (ipgroup_id={ipgroup_id}, nfa_uuid={nfa_uuid}): {e}")
        return []

def calculate_95th_percentile(data, direction='both'):
    """计算95值
    
    Args:
        data: 流速数据列表
        direction: 流量方向，'send', 'recv' 或 'both'
    
    Returns:
        95值 (Mbps)
    """
    if not data:
        return 0
    
    # 转换为DataFrame
    df = pd.DataFrame(data)
    
    # 将流量从字节转换为Mbps (Megabits per second)，按照单位换算方式.txt中的公式
    df['recv_mbps'] = df['recv'] * 8 / 60 / 1024 / 1024  # bytes * 8 / 60 / 1024 / 1024
    df['send_mbps'] = df['send'] * 8 / 60 / 1024 / 1024  # bytes * 8 / 60 / 1024 / 1024
    
    # 根据方向计算95值
    if direction == 'recv':
        values = df['recv_mbps'].values
    elif direction == 'send':
        values = df['send_mbps'].values
    else:  # both - 取每个时间点的收发和
        values = (df['recv_mbps'] + df['send_mbps']).values
    
    # 使用 np.partition 在 O(n) 时间内得到等价结果：
    # 取“底部95%中的最大值”，即升序第 k 个元素（k = ceil(0.95*n) - 1）
    n = len(values)
    if n == 0:
        return 0
    k = int(np.ceil(n * 0.95)) - 1
    if k < 0:
        k = 0
    if k >= n:
        k = n - 1
    part = np.partition(values, k)
    return float(part[k])

def calculate_95th_from_series(series: pd.Series) -> float:
    """对已是 Mbps 的一维 Series 计算 95 值（同上定义）。"""
    values = series.dropna().to_numpy()
    n = len(values)
    if n == 0:
        return 0.0
    k = int(np.ceil(n * 0.95)) - 1
    if k < 0:
        k = 0
    if k >= n:
        k = n - 1
    part = np.partition(values, k)
    return float(part[k])

def fetch_speed_data_for_pairs_raw(connection, pairs, start_time, end_time, batch_size=200):
    """
    批量拉取多所院校在时间范围内的原始 5 分钟数据，返回 DataFrame：
    列 [ipgroup_id, nfa_uuid, create_time, recv, send]
    为避免单次 SQL 过长，按 batch_size 分批查询后合并。
    """
    if not pairs:
        return pd.DataFrame()
    frames = []
    total = len(pairs)
    for i in range(0, total, batch_size):
        chunk = pairs[i:i + batch_size]
        placeholders = ", ".join(["(%s, %s)"] * len(chunk))
        sql = f"""
            SELECT ipgroup_id, nfa_uuid, create_time, recv, send
            FROM nfa_ip_group_speed_logs_5m
            WHERE create_time BETWEEN %s AND %s
              AND (ipgroup_id, nfa_uuid) IN ({placeholders})
            ORDER BY ipgroup_id, nfa_uuid, create_time
        """
        params = [start_time, end_time]
        for ipg, uuid in chunk:
            params.extend([ipg, uuid])
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                rows = cursor.fetchall()
                if rows:
                    df = pd.DataFrame(rows)
                    frames.append(df)
        except Exception as e:
            logger.error(f"批量拉取速度数据失败: {e}")
            continue
    if not frames:
        return pd.DataFrame()
    df_all = pd.concat(frames, ignore_index=True)
    df_all['create_time'] = pd.to_datetime(df_all['create_time'])
    return df_all

def process_schools_batched(connection, schools, start_time, end_time, direction, export_daily, batch_size=200):
    """
    批量方式计算逐校（或逐校按天）95 值，显著减少数据库往返。
    返回与 process_schools 相同结构的结果列表。
    """
    results = []
    if not schools:
        return results
    pairs = [(s['ipgroup_id'], s['nfa_uuid']) for s in schools]
    # 构建 (ipgroup_id, nfa_uuid) -> 学校信息映射
    info_map = {(s['ipgroup_id'], s['nfa_uuid']): s for s in schools}
    df = fetch_speed_data_for_pairs_raw(connection, pairs, start_time, end_time, batch_size=batch_size)
    if df.empty:
        return results
    # 统一转换 Mbps
    df['recv_mbps'] = df['recv'] * 8 / 60 / 1024 / 1024
    df['send_mbps'] = df['send'] * 8 / 60 / 1024 / 1024
    if export_daily:
        df['date'] = df['create_time'].dt.date
        grp_keys = ['ipgroup_id', 'nfa_uuid', 'date']
        for (ipg, uuid, date_obj), g in df.groupby(grp_keys, sort=False):
            s = info_map.get((ipg, uuid), {})
            if direction == 'recv':
                series = g['recv_mbps']
            elif direction == 'send':
                series = g['send_mbps']
            else:
                series = g['recv_mbps'] + g['send_mbps']
            val = calculate_95th_from_series(series)
            results.append({
                'school_id': s.get('school_id', ''),
                'ipgroup_name': s.get('ipgroup_name', ''),
                'ipgroup_id': ipg,
                'nfa_uuid': uuid,
                'date': date_obj.strftime('%Y-%m-%d'),
                'daily_95th_percentile_mbps': val,
                'direction': direction,
                'data_points_daily': int(series.shape[0])
            })
    else:
        grp_keys = ['ipgroup_id', 'nfa_uuid']
        for (ipg, uuid), g in df.groupby(grp_keys, sort=False):
            s = info_map.get((ipg, uuid), {})
            if direction == 'recv':
                series = g['recv_mbps']
            elif direction == 'send':
                series = g['send_mbps']
            else:
                series = g['recv_mbps'] + g['send_mbps']
            val = calculate_95th_from_series(series)
            results.append({
                'school_id': s.get('school_id', ''),
                'ipgroup_name': s.get('ipgroup_name', ''),
                'ipgroup_id': ipg,
                'nfa_uuid': uuid,
                '95th_percentile_mbps': val,
                'data_points': int(series.shape[0]),
                'direction': direction
            })
    return results

def compute_month_rows(data_cfg: dict, schools, start_s: str, end_s: str, direction: str, batch_size: int,
                       grouped: bool, cp_map: dict, month_label: str):
    conn = connect_to_db(data_cfg)
    try:
        rows = []
        if grouped:
            rows = compute_grouped_results_batched(
                conn, schools, start_s, end_s, direction,
                export_daily=False, batch_size=batch_size
            )
            for item in rows:
                item['month'] = month_label
        else:
            batch_results = process_schools_batched(
                conn, schools, start_s, end_s, direction, export_daily=False, batch_size=batch_size
            )
            for item in batch_results:
                item['month'] = month_label
                item['cp'] = cp_map.get((item.get('ipgroup_id'), item.get('nfa_uuid')))
            rows = batch_results
        return rows
    finally:
        conn.close()

# 新增：通用处理与保存函数，避免重复代码
def _split_names_to_set(names_str):
    if not names_str:
        return set()
    return {n.strip() for n in names_str.split(',') if n.strip()}

def process_schools(connection, schools, start_time, end_time, direction, export_daily):
    """按给定学校列表计算95值，返回结果列表"""
    results = []
    if not schools:
        return results

    if export_daily:
        logger.info("开始处理每日95值数据...")
        for school in schools:
            logger.info(f"正在处理院校: {school['ipgroup_name']} (每日95值)")
            speed_data = get_speed_data(
                connection,
                school['ipgroup_id'],
                school['nfa_uuid'],
                start_time,
                end_time
            )
            if not speed_data:
                logger.warning(f"未找到院校 {school['ipgroup_name']} (ID: {school['school_id']}) 在指定时间范围内的流速数据")
                continue

            df_speed = pd.DataFrame(speed_data)
            df_speed['create_time'] = pd.to_datetime(df_speed['create_time'])
            df_speed['date'] = df_speed['create_time'].dt.date

            daily_groups = df_speed.groupby('date')
            for date_obj, group_data in daily_groups:
                daily_95th_value = calculate_95th_percentile(group_data.to_dict('records'), direction)
                results.append({
                    'school_id': school['school_id'],
                    'ipgroup_name': school['ipgroup_name'],
                    'ipgroup_id': school['ipgroup_id'],
                    'nfa_uuid': school['nfa_uuid'],
                    'date': date_obj.strftime('%Y-%m-%d'),
                    'daily_95th_percentile_mbps': daily_95th_value,
                    'direction': direction,
                    'data_points_daily': len(group_data)
                })
    else:
        logger.info("开始处理周期汇总95值数据...")
        for school in schools:
            logger.info(f"正在处理院校: {school['ipgroup_name']} (周期95值)")
            speed_data = get_speed_data(
                connection,
                school['ipgroup_id'],
                school['nfa_uuid'],
                start_time,
                end_time
            )
            if not speed_data:
                logger.warning(f"未找到院校 {school['ipgroup_name']} (ID: {school['school_id']}) 在指定时间范围内的流速数据，已跳过写入。")
                continue

            percentile_95 = calculate_95th_percentile(speed_data, direction)
            results.append({
                'school_id': school['school_id'],
                'ipgroup_name': school['ipgroup_name'],
                'ipgroup_id': school['ipgroup_id'],
                'nfa_uuid': school['nfa_uuid'],
                '95th_percentile_mbps': percentile_95,
                'data_points': len(speed_data),
                'direction': direction
            })
    return results

def aggregate_speed_data_for_schools(connection, schools, start_time, end_time):
    frames = []
    for school in schools:
        data = get_speed_data(connection, school['ipgroup_id'], school['nfa_uuid'], start_time, end_time)
        if not data:
            continue
        df = pd.DataFrame(data)
        if df.empty:
            continue
        frames.append(df[['create_time', 'recv', 'send']])
    if not frames:
        return pd.DataFrame()
    df_all = pd.concat(frames, ignore_index=True)
    df_all['create_time'] = pd.to_datetime(df_all['create_time'])
    df_agg = df_all.groupby('create_time', as_index=False)[['recv', 'send']].sum().sort_values('create_time')
    return df_agg

def aggregate_speed_data_for_pairs_db(connection, pairs, start_time, end_time):
    """
    pairs: List of (ipgroup_id, nfa_uuid)
    返回 DataFrame: columns [create_time, recv, send]
    """
    if not pairs:
        return pd.DataFrame()
    # 构造多列 IN ((%s,%s),...) 占位符
    placeholders = ", ".join(["(%s, %s)"] * len(pairs))
    sql = f"""
        SELECT create_time,
               SUM(recv) AS recv,
               SUM(send) AS send
        FROM nfa_ip_group_speed_logs_5m
        WHERE create_time BETWEEN %s AND %s
          AND (ipgroup_id, nfa_uuid) IN ({placeholders})
        GROUP BY create_time
        ORDER BY create_time
    """
    params = [start_time, end_time]
    for ipg, uuid in pairs:
        params.extend([ipg, uuid])
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows)
            df['create_time'] = pd.to_datetime(df['create_time'])
            return df
    except Exception as e:
        logger.error(f"数据库端聚合剩余院校失败: {e}")
        return pd.DataFrame()

def aggregate_all_and_compute(connection, schools, start_time, end_time, direction, export_daily):
    """
    对所有学校在相同时间点上进行汇总（recv/send 求和）后计算：
    - 周期模式：输出 1 行“全部院校汇总”
    - 每日模式：按天输出“全部院校汇总”
    优先在数据库端按时间聚合以减少数据量。
    """
    if not schools:
        return []
    pairs = [(s['ipgroup_id'], s['nfa_uuid']) for s in schools]
    df_agg = aggregate_speed_data_for_pairs_db(connection, pairs, start_time, end_time)
    if df_agg.empty:
        # 回退到 Python 端聚合
        df_raw = fetch_speed_data_for_pairs_raw(connection, pairs, start_time, end_time)
        if df_raw.empty:
            return []
        df_agg = df_raw.groupby('create_time', as_index=False)[['recv', 'send']].sum().sort_values('create_time')
    # 转换 Mbps
    df_agg['recv_mbps'] = df_agg['recv'] * 8 / 60 / 1024 / 1024
    df_agg['send_mbps'] = df_agg['send'] * 8 / 60 / 1024 / 1024
    if export_daily:
        df_agg['date'] = df_agg['create_time'].dt.date
        results = []
        for date_obj, g in df_agg.groupby('date', sort=False):
            if direction == 'recv':
                series = g['recv_mbps']
            elif direction == 'send':
                series = g['send_mbps']
            else:
                series = g['recv_mbps'] + g['send_mbps']
            val = calculate_95th_from_series(series)
            results.append({
                'school_id': '',
                'ipgroup_name': '全部院校汇总',
                'ipgroup_id': '',
                'nfa_uuid': '',
                'date': date_obj.strftime('%Y-%m-%d'),
                'daily_95th_percentile_mbps': val,
                'direction': direction,
                'data_points_daily': int(series.shape[0])
            })
        return results
    else:
        if direction == 'recv':
            series = df_agg['recv_mbps']
        elif direction == 'send':
            series = df_agg['send_mbps']
        else:
            series = df_agg['recv_mbps'] + df_agg['send_mbps']
        val = calculate_95th_from_series(series)
        return [{
            'school_id': '',
            'ipgroup_name': '全部院校汇总',
            'ipgroup_id': '',
            'nfa_uuid': '',
            '95th_percentile_mbps': val,
            'data_points': int(series.shape[0]),
            'direction': direction
        }]

def _group_pairs_by_school(schools):
    """
    将院校按 (region, cp, school_name) 归组，返回 { (region, cp, school_name): [(ipgroup_id, nfa_uuid), ...] }
    """
    groups = {}
    for s in schools:
        region = s.get('region') or ''
        cp = s.get('cp') or ''
        name = s.get('school_name') or (s.get('ipgroup_name') or '')
        key = (region, cp, name)
        groups.setdefault(key, []).append((s['ipgroup_id'], s['nfa_uuid']))
    return groups

def _compute_95_from_agg_df(df_agg: pd.DataFrame, direction: str) -> tuple[float, int]:
    if df_agg.empty:
        return 0.0, 0
    df_agg = df_agg.copy()
    df_agg['recv_mbps'] = df_agg['recv'] * 8 / 60 / 1024 / 1024
    df_agg['send_mbps'] = df_agg['send'] * 8 / 60 / 1024 / 1024
    if direction == 'recv':
        series = df_agg['recv_mbps']
    elif direction == 'send':
        series = df_agg['send_mbps']
    else:
        series = df_agg['recv_mbps'] + df_agg['send_mbps']
    return calculate_95th_from_series(series), int(series.shape[0])

def compute_grouped_results(connection, schools, start_time, end_time, direction):
    """按 (region, cp, school_name) 聚合不同 ipgroup 的时间序列后计算 95 值。"""
    groups = _group_pairs_by_school(schools)
    rows = []
    for (region, cp, name), pairs in groups.items():
        df_agg = aggregate_speed_data_for_pairs_db(connection, pairs, start_time, end_time)
        val, cnt = _compute_95_from_agg_df(df_agg, direction)
        rows.append({
            'region': region,
            'cp': cp,
            'school_name': name,
            'ipgroup_name': name,
            '95th_percentile_mbps': val,
            'data_points': cnt,
            'direction': direction
        })
    return rows

def compute_grouped_results_batched(connection, schools, start_time, end_time, direction, export_daily=False, batch_size=200):
    """
    更高效的分组聚合：一次（分批）拉取所有学校的原始5分钟数据，再在内存中按
    (region, cp, school_name) 与时间进行聚合，最后计算95值。
    """
    if not schools:
        return []
    pairs = [(s['ipgroup_id'], s['nfa_uuid']) for s in schools]
    # 建立映射 (ipgroup_id, nfa_uuid) -> (region, cp, school_name)
    meta_rows = []
    for s in schools:
        meta_rows.append({
            'ipgroup_id': s['ipgroup_id'],
            'nfa_uuid': s['nfa_uuid'],
            'region': s.get('region') or '',
            'cp': s.get('cp') or '',
            'school_name': s.get('school_name') or (s.get('ipgroup_name') or '')
        })
    df_meta = pd.DataFrame(meta_rows)
    df_raw = fetch_speed_data_for_pairs_raw(connection, pairs, start_time, end_time, batch_size=batch_size)
    if df_raw.empty:
        return []
    # 关联元数据，得到分组键
    df = df_raw.merge(df_meta, on=['ipgroup_id', 'nfa_uuid'], how='left')
    # 转换 Mbps
    df['recv_mbps'] = df['recv'] * 8 / 60 / 1024 / 1024 if 'recv' in df.columns else 0.0
    df['send_mbps'] = df['send'] * 8 / 60 / 1024 / 1024 if 'send' in df.columns else 0.0
    # 先按时间点聚合（同一院校同一CP下不同实例在同一 create_time 上求和）
    if direction == 'recv':
        df['metric'] = df['recv_mbps']
    elif direction == 'send':
        df['metric'] = df['send_mbps']
    else:
        df['metric'] = df['recv_mbps'] + df['send_mbps']
    df_agg_time = df.groupby(['region', 'cp', 'school_name', 'create_time'], as_index=False)['metric'].sum()
    results = []
    if export_daily:
        df_agg_time['date'] = df_agg_time['create_time'].dt.date
        for (region, cp, name, date_obj), g in df_agg_time.groupby(['region', 'cp', 'school_name', 'date'], sort=False):
            val = calculate_95th_from_series(g['metric'])
            results.append({
                'region': region,
                'cp': cp,
                'school_name': name,
                'ipgroup_name': name,
                'date': date_obj.strftime('%Y-%m-%d'),
                'daily_95th_percentile_mbps': val,
                'direction': direction,
                'data_points_daily': int(g['metric'].shape[0])
            })
    else:
        for (region, cp, name), g in df_agg_time.groupby(['region', 'cp', 'school_name'], sort=False):
            val = calculate_95th_from_series(g['metric'])
            results.append({
                'region': region,
                'cp': cp,
                'school_name': name,
                'ipgroup_name': name,
                '95th_percentile_mbps': val,
                'data_points': int(g['metric'].shape[0]),
                'direction': direction
            })
    return results

def compute_grouped_results_daily(connection, schools, start_time, end_time, direction):
    """按 (region, cp, school_name) 聚合后按天输出 95 值。"""
    groups = _group_pairs_by_school(schools)
    rows = []
    for (region, cp, name), pairs in groups.items():
        df_agg = aggregate_speed_data_for_pairs_db(connection, pairs, start_time, end_time)
        if df_agg.empty:
            continue
        df_agg = df_agg.copy()
        df_agg['date'] = pd.to_datetime(df_agg['create_time']).dt.date
        for date_obj, g in df_agg.groupby('date'):
            val, cnt = _compute_95_from_agg_df(g, direction)
            rows.append({
                'region': region,
                'cp': cp,
                'school_name': name,
                'ipgroup_name': name,
                'date': date_obj.strftime('%Y-%m-%d'),
                'daily_95th_percentile_mbps': val,
                'direction': direction,
                'data_points_daily': cnt
            })
    return rows

def save_results(results, output_path, is_daily, direction, start_time, end_time, extra_log_prefix="", sort_by=None, sort_order='desc'):
    if not results:
        logger.warning(f"{extra_log_prefix}没有计算任何结果，跳过写入文件。")
        return
    df_final_results = pd.DataFrame(results)
    # 如需排序则按指定字段排序
    if sort_by:
        if sort_by in df_final_results.columns:
            ascending = (sort_order == 'asc')
            try:
                df_final_results = df_final_results.sort_values(by=sort_by, ascending=ascending)
                logger.info(f"{extra_log_prefix}已按字段 '{sort_by}' 进行{'升序' if ascending else '降序'}排序。")
            except Exception as e:
                logger.warning(f"{extra_log_prefix}按字段 '{sort_by}' 排序失败：{e}，将保持原顺序。")
        else:
            logger.warning(f"{extra_log_prefix}指定的排序字段 '{sort_by}' 不存在，将保持原顺序。可用字段：{', '.join(df_final_results.columns)}")
    df_final_results.to_csv(output_path, index=False, encoding='utf-8-sig')
    if is_daily:
        logger.info(f"{extra_log_prefix}每日95值结果已保存到 {output_path}")
    else:
        logger.info(f"{extra_log_prefix}周期汇总95值结果已保存到 {output_path}")
        logger.info("汇总信息:")
        logger.info(f"  时间范围: {start_time} - {end_time}")
        logger.info(f"  流量方向: {direction}")
        logger.info(f"  总院校数: {len(df_final_results)}")
        if '95th_percentile_mbps' in df_final_results.columns and not df_final_results.empty:
            logger.info(f"  平均95值 (Mbps): {df_final_results['95th_percentile_mbps'].mean():.2f}")
            max_95_school = df_final_results.loc[df_final_results['95th_percentile_mbps'].idxmax()]
            logger.info(f"  最大95值 (Mbps): {max_95_school['95th_percentile_mbps']:.2f} (院校: {max_95_school['ipgroup_name']})")

def main():
    """主函数"""
    args = parse_args()
    logger.info(f"脚本模式: {'导出每日95值' if args.export_daily else '计算周期汇总95值'}")

    # 年度模式：按月导出每个院校、每个CP的95值
    def _month_ranges(year: int):
        ranges = []
        for m in range(1, 13):
            start_dt = datetime(year, m, 1, 0, 0, 0)
            if m == 12:
                next_first = datetime(year + 1, 1, 1, 0, 0, 0)
            else:
                next_first = datetime(year, m + 1, 1, 0, 0, 0)
            end_dt = next_first - timedelta(seconds=1)
            ranges.append({
                'label': f"{year}-{m:02d}",
                'start': start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'end': end_dt.strftime('%Y-%m-%d %H:%M:%S')
            })
        return ranges

    # 准备 DB 连接（关系库 + 流量库）
    rel_cfg, data_cfg = _load_dual_db_configs(args)
    rel_conn = connect_to_db(rel_cfg)
    data_conn = connect_to_db(data_cfg)

    try:
        if args.year:
            # 年度模式无需日导出
            if args.export_daily:
                logger.warning("年度模式下忽略 --export-daily 参数：将按月导出。")
            # 获取学校（可跨CP）
            schools = get_schools_by_province_and_cp(rel_conn, args.province, args.cp, args.school)
            if not schools:
                logger.warning(f"未找到符合条件的院校 (省份='{args.province}', CP='{args.cp or 'ALL'}')")
                sys.exit(0)
            # 排除名单
            if args.exclude_school:
                exclude_set = _split_names_to_set(args.exclude_school)
                if exclude_set:
                    schools = [s for s in schools if s.get('school_name') not in exclude_set]
            # 建立 (ipgroup_id, nfa_uuid) -> cp 的映射
            cp_map = {(s['ipgroup_id'], s['nfa_uuid']): s.get('cp') for s in schools}
            all_rows = []
            ranges = list(_month_ranges(args.year))
            if args.jobs and args.jobs > 1:
                logger.info(f"年度模式启用并行：{args.jobs} 线程")
                with ThreadPoolExecutor(max_workers=args.jobs) as ex:
                    futures = []
                    for rg in ranges:
                        futures.append(ex.submit(
                            compute_month_rows,
                            data_cfg,
                            schools,
                            rg['start'],
                            rg['end'],
                            args.direction,
                            args.batch_size,
                            args.group_by_school,
                            cp_map,
                            rg['label']
                        ))
                    for fut in as_completed(futures):
                        try:
                            rows = fut.result()
                            if rows:
                                all_rows.extend(rows)
                        except Exception as e:
                            logger.error(f"并行月份任务失败: {e}")
            else:
                for rg in ranges:
                    month_label = rg['label']
                    start_s, end_s = rg['start'], rg['end']
                    logger.info(f"[{month_label}] 开始计算: {start_s} ~ {end_s}")
                    rows = compute_month_rows(
                        data_cfg, schools, start_s, end_s, args.direction,
                        args.batch_size, args.group_by_school, cp_map, month_label
                    )
                    all_rows.extend(rows)
            if not all_rows:
                logger.warning("全年无任何计算结果。")
                sys.exit(0)
            df = pd.DataFrame(all_rows)
            # 排序（并行时确保输出稳定按月/学校/CP）
            sort_keys = ['month']
            if 'cp' in df.columns:
                sort_keys.append('cp')
            if 'school_name' in df.columns:
                sort_keys.append('school_name')
            elif 'ipgroup_name' in df.columns:
                sort_keys.append('ipgroup_name')
            df = df.sort_values(by=sort_keys, kind='stable')
            # 调整列顺序，便于查看
            preferred = ['month', 'cp', 'school_name', 'ipgroup_name', 'school_id', 'ipgroup_id', 'nfa_uuid', '95th_percentile_mbps', 'data_points', 'direction']
            cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
            df = df[cols]
            # 输出文件名
            out_path = args.output
            if out_path == '95th_percentile_results.csv':
                out_path = f"95th_percentile_{args.province}_{args.year}_monthly.csv"
            df.to_csv(out_path, index=False, encoding='utf-8-sig')
            logger.info(f"年度按月结果已保存到 {out_path}")
            return

        # 非年度模式：要求 cp、start/end 存在
        if not args.cp:
            logger.error("未指定 --year 时，必须提供 --cp。")
            sys.exit(1)
        if not args.start_time or not args.end_time:
            logger.error("未指定 --year 时，必须提供 --start-time 与 --end-time。")
            sys.exit(1)
        try:
            start_time = datetime.strptime(args.start_time, '%Y-%m-%d %H:%M:%S')
            end_time = datetime.strptime(args.end_time, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            logger.error("时间格式错误，请使用 YYYY-MM-DD HH:MM:SS 格式")
            sys.exit(1)

        # 获取符合条件的院校（先按 --school 过滤，再根据 --exclude-school 划分两组）
        schools = get_schools_by_province_and_cp(rel_conn, args.province, args.cp, args.school)
        
        if not schools:
            warning_msg = f"未找到符合条件的院校 (省份='{args.province}', CP='{args.cp}')"
            if args.school:
                warning_msg += f", 院校='{args.school}'"
            logger.warning(warning_msg)
            sys.exit(0)
        
        # 如果提供了 --exclude-school，则分别计算两组：被排除组 与 剩余组（剩余组进行汇总后计算）
        if args.exclude_school:
            exclude_set = _split_names_to_set(args.exclude_school)
            if not exclude_set:
                logger.warning("提供的 --exclude-school 参数为空或格式不正确，将按未提供处理。")
            
            excluded_schools = [s for s in schools if s.get('school_name') in exclude_set]
            remaining_schools = [s for s in schools if s.get('school_name') not in exclude_set]

            # 输出文件名约定：在给定 --output 的基础上增加后缀 _excluded 与 _remaining
            root, ext = os.path.splitext(args.output)
            ext = ext if ext else '.csv'
            out_excluded = f"{root}_excluded{ext}"
            out_remaining = f"{root}_remaining{ext}"

            # 1) 排除组：逐校计算（保持原有行为）
            if excluded_schools:
                logger.info(f"将对排除院校单独计算，共 {len(excluded_schools)} 所。名单: {', '.join(sorted(exclude_set))}")
                results_excluded = process_schools(data_conn, excluded_schools, args.start_time, args.end_time, args.direction, args.export_daily)
                save_results(results_excluded, out_excluded, args.export_daily, args.direction, args.start_time, args.end_time, extra_log_prefix="[排除组] ", sort_by=args.sortby, sort_order=args.sort_order)
            else:
                logger.warning("未在查询结果中找到需要排除并单独计算的院校，跳过排除组计算。")

            # 2) 剩余组：先将所有学校的流量在时间点上汇总，再计算整体95值
            if remaining_schools:
                logger.info(f"将对剩余院校进行整体汇总后计算（不是逐校），共 {len(remaining_schools)} 所。")
                # 统计剩余院校名称（优先 ipgroup_name，其次 school_name），并合并同名计算数量
                name_list = [
                    (s.get('ipgroup_name') or s.get('school_name') or '').strip()
                    for s in remaining_schools
                ]
                name_list = [n for n in name_list if n]
                name_counter = Counter(name_list)
                if name_counter:
                    items = sorted(name_counter.items(), key=lambda x: x[0])
                    pretty = ", ".join([f"{n} x{c}" if c > 1 else n for n, c in items])
                    logger.info("剩余院校名单(同名合并统计): " + pretty)
                    logger.info(f"剩余院校唯一名称数: {len(items)}")
                else:
                    logger.info("剩余院校名单为空")

                # 将剩余院校名单导出为TXT文件
                out_remaining_names = f"{root}_remaining_names.txt"
                try:
                    items = sorted(name_counter.items(), key=lambda x: x[0])
                    with open(out_remaining_names, 'w', encoding='utf-8-sig') as f:
                        for n, c in items:
                            line = f"{n} x{c}" if c > 1 else n
                            f.write(line + "\n")
                    logger.info(f"已将剩余院校名单导出到 {out_remaining_names} (共 {len(items)} 个唯一名称，原始 {len(remaining_schools)} 条)")
                except Exception as e:
                    logger.error(f"导出剩余院校名单失败: {e}")

                # 优先在数据库端完成按时间聚合
                pairs = [(s['ipgroup_id'], s['nfa_uuid']) for s in remaining_schools]
                df_agg = aggregate_speed_data_for_pairs_db(data_conn, pairs, args.start_time, args.end_time)
                if df_agg.empty:
                    # 回退到Python端聚合
                    df_agg = aggregate_speed_data_for_schools(data_conn, remaining_schools, args.start_time, args.end_time)

                if df_agg.empty:
                    logger.warning("剩余院校在时间范围内没有数据，跳过剩余组计算。")
                else:
                    if args.export_daily:
                        df_agg['date'] = df_agg['create_time'].dt.date
                        results_remaining = []
                        for date_obj, group in df_agg.groupby('date'):
                            val = calculate_95th_percentile(group.to_dict('records'), args.direction)
                            results_remaining.append({
                                'school_id': '',
                                'ipgroup_name': '剩余院校汇总',
                                'ipgroup_id': '',
                                'nfa_uuid': '',
                                'date': date_obj.strftime('%Y-%m-%d'),
                                'daily_95th_percentile_mbps': val,
                                'direction': args.direction,
                                'data_points_daily': len(group)
                            })
                        save_results(results_remaining, out_remaining, True, args.direction, args.start_time, args.end_time, extra_log_prefix="[剩余组-汇总] ", sort_by=args.sortby, sort_order=args.sort_order)
                    else:
                        val = calculate_95th_percentile(df_agg.to_dict('records'), args.direction)
                        results_remaining = [{
                            'school_id': '',
                            'ipgroup_name': '剩余院校汇总',
                            'ipgroup_id': '',
                            'nfa_uuid': '',
                            '95th_percentile_mbps': val,
                            'data_points': len(df_agg),
                            'direction': args.direction
                        }]
                        save_results(results_remaining, out_remaining, False, args.direction, args.start_time, args.end_time, extra_log_prefix="[剩余组-汇总] ", sort_by=args.sortby, sort_order=args.sort_order)
            else:
                logger.warning("排除后无剩余院校可计算，跳过剩余组计算。")
        else:
            # 支持整体汇总与批量逐校两种快速路径
            if args.group_by_school:
                if args.export_daily:
                    logger.info("启用 --group-by-school 快速批量模式：按 (region, cp, school_name) 聚合后按天输出95值")
                    results = compute_grouped_results_batched(
                        data_conn, schools, args.start_time, args.end_time, args.direction,
                        export_daily=True, batch_size=args.batch_size
                    )
                else:
                    logger.info("启用 --group-by-school 快速批量模式：按 (region, cp, school_name) 聚合后输出周期95值")
                    results = compute_grouped_results_batched(
                        data_conn, schools, args.start_time, args.end_time, args.direction,
                        export_daily=False, batch_size=args.batch_size
                    )
            elif args.aggregate_all:
                logger.info("启用 --aggregate-all：将所有符合条件的院校在相同时间点上汇总后计算95值")
                results = aggregate_all_and_compute(data_conn, schools, args.start_time, args.end_time, args.direction, args.export_daily)
            else:
                logger.info("启用批量拉取快速模式：逐校（或逐校按天）在内存中计算95值，减少数据库往返")
                results = process_schools_batched(data_conn, schools, args.start_time, args.end_time, args.direction, args.export_daily, batch_size=args.batch_size)

            if args.export_daily:
                save_results(results, args.output, True, args.direction, args.start_time, args.end_time, sort_by=args.sortby, sort_order=args.sort_order)
            else:
                save_results(results, args.output, False, args.direction, args.start_time, args.end_time, sort_by=args.sortby, sort_order=args.sort_order)
                logger.info("汇总信息:")
                logger.info(f"  省份: {args.province}")
                logger.info(f"  CP类型: {args.cp}")
                if args.school:
                    logger.info(f"  指定院校: {args.school}")
                logger.info(f"  时间范围: {args.start_time} - {args.end_time}")
                logger.info(f"  流量方向: {args.direction}")

    except Exception as e:
        logger.error(f"处理过程中发生错误: {e}")
    finally:
        try:
            if rel_conn:
                rel_conn.close()
        finally:
            if data_conn:
                data_conn.close()

if __name__ == "__main__":
    main()
