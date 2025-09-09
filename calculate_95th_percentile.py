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

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("95值计算工具")

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='计算指定省份、指定CP类型、指定时间范围内所有院校的95值')
    parser.add_argument('--province', '-p', required=True, help='指定省份，例如：四川')
    parser.add_argument('--cp', '-c', required=True, help='指定CP类型，例如：教育网')
    parser.add_argument('--start-time', '-s', required=True, help='开始时间，格式：YYYY-MM-DD HH:MM:SS')
    parser.add_argument('--end-time', '-e', required=True, help='结束时间，格式：YYYY-MM-DD HH:MM:SS')
    parser.add_argument('--config', default='db_config.ini', help='数据库配置文件路径')
    parser.add_argument('--output', '-o', default='95th_percentile_results.csv', help='输出结果文件路径')
    parser.add_argument('--direction', '-d', default='both', choices=['send', 'recv', 'both'], 
                        help='流量方向：send(发送)、recv(接收)或both(双向)')
    parser.add_argument('--school', '-sc', help='指定院校名称，多个院校用逗号分隔，例如：电子科技大学,四川大学')
    parser.add_argument('--export-daily', action='store_true', help='导出每日95值，而不是整个周期的汇总95值')
    parser.add_argument('--exclude-school', '-esc', help='排除的院校名称，多个院校用逗号分隔，例如：电子科技大学,四川大学')
    return parser.parse_args()

def load_db_config(config_file):
    """加载数据库配置"""
    if not os.path.exists(config_file):
        logger.error(f"配置文件 {config_file} 不存在")
        create_default_config(config_file)
        logger.info(f"已创建默认配置文件 {config_file}，请修改后重新运行")
        sys.exit(1)
    
    config = configparser.ConfigParser()
    config.read(config_file)
    return {
        'host': config.get('DATABASE', 'host'),
        'port': config.getint('DATABASE', 'port'),
        'user': config.get('DATABASE', 'user'),
        'password': config.get('DATABASE', 'password'),
        'db': config.get('DATABASE', 'db'),
        'charset': config.get('DATABASE', 'charset', fallback='utf8mb4')
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

def get_schools_by_province_and_cp(connection, province, cp, school_names_str=None):
    """获取指定省份、CP类型以及可选的指定院校的所有院校（仅 type='yuanxiao'）"""
    base_query = """
    SELECT DISTINCT school_id, school_name, ipgroup_name, ipgroup_id, nfa_uuid
    FROM nfa_ipgroup
    WHERE region = %s AND cp = %s AND type = %s
    """
    params = [province, cp, 'yuanxiao']

    if school_names_str:
        school_names_list = [name.strip() for name in school_names_str.split(',') if name.strip()]
        if school_names_list:
            placeholders = ', '.join(['%s'] * len(school_names_list))
            base_query += f" AND school_name IN ({placeholders})"
            params.extend(school_names_list)
            logger.info(f"筛选条件：省份='{province}', CP='{cp}', type='yuanxiao', 指定院校='{school_names_str}'")
        else:
            logger.warning("提供的 --school 参数值为空或格式不正确，将忽略院校名称筛选。")
            logger.info(f"筛选条件：省份='{province}', CP='{cp}', type='yuanxiao' (未指定有效院校)")
    else:
        logger.info(f"筛选条件：省份='{province}', CP='{cp}', type='yuanxiao' (未指定院校)")

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
    
    # 排序并计算95值
    sorted_values = np.sort(values)[::-1]  # 从大到小排序
    
    # 计算95百分位的索引
    n = len(sorted_values)
    if n == 0:
        return 0
    
    # 舍弃前5%的点，取剩下的最大的点
    index_95th = int(n * 0.05)
    if index_95th >= n:
        index_95th = n - 1
    
    return sorted_values[index_95th]

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
                logger.warning(f"未找到院校 {school['ipgroup_name']} (ID: {school['school_id']}) 在指定时间范围内的流速数据")
                results.append({
                    'school_id': school['school_id'],
                    'ipgroup_name': school['ipgroup_name'],
                    'ipgroup_id': school['ipgroup_id'],
                    'nfa_uuid': school['nfa_uuid'],
                    '95th_percentile_mbps': 0,
                    'data_points': 0,
                    'direction': direction
                })
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

def save_results(results, output_path, is_daily, direction, start_time, end_time, extra_log_prefix=""):
    if not results:
        logger.warning(f"{extra_log_prefix}没有计算任何结果，跳过写入文件。")
        return
    df_final_results = pd.DataFrame(results)
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
    
    # 解析时间
    try:
        start_time = datetime.strptime(args.start_time, '%Y-%m-%d %H:%M:%S')
        end_time = datetime.strptime(args.end_time, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        logger.error("时间格式错误，请使用 YYYY-MM-DD HH:MM:SS 格式")
        sys.exit(1)
    
    # 加载数据库配置
    db_config = load_db_config(args.config)
    
    # 连接数据库
    connection = connect_to_db(db_config)
    
    try:
        # 获取符合条件的院校（先按 --school 过滤，再根据 --exclude-school 划分两组）
        schools = get_schools_by_province_and_cp(connection, args.province, args.cp, args.school)
        
        if not schools:
            warning_msg = f"未找到符合条件的院校 (省份='{args.province}', CP='{args.cp}'"
            if args.school:
                warning_msg += f", 院校='{args.school}'"
            warning_msg += ")"
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
                results_excluded = process_schools(connection, excluded_schools, start_time, end_time, args.direction, args.export_daily)
                save_results(results_excluded, out_excluded, args.export_daily, args.direction, start_time, end_time, extra_log_prefix="[排除组] ")
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
                    # 打印带数量的名单，例如：学校A x2, 学校B
                    items = sorted(name_counter.items(), key=lambda x: x[0])
                    pretty = ", ".join([f"{n} x{c}" if c > 1 else n for n, c in items])
                    logger.info("剩余院校名单(同名合并统计): " + pretty)
                    logger.info(f"剩余院校唯一名称数: {len(items)}")
                else:
                    logger.info("剩余院校名单为空")

                # 将剩余院校名单导出为TXT文件
                out_remaining_names = f"{root}_remaining_names.txt"
                try:
                    # TXT 导出：优先 ipgroup_name，其次 school_name；同名合并并显示数量（>1 则追加 xN）
                    items = sorted(name_counter.items(), key=lambda x: x[0])
                    with open(out_remaining_names, 'w', encoding='utf-8-sig') as f:
                        for n, c in items:
                            line = f"{n} x{c}" if c > 1 else n
                            f.write(line + "\n")
                    logger.info(f"已将剩余院校名单导出到 {out_remaining_names} (共 {len(items)} 个唯一名称，原始 {len(remaining_schools)} 条)")
                except Exception as e:
                    logger.error(f"导出剩余院校名单失败: {e}")

                # 优先在数据库端完成按时间聚合，显著减少数据量与Python端开销
                pairs = [(s['ipgroup_id'], s['nfa_uuid']) for s in remaining_schools]
                df_agg = aggregate_speed_data_for_pairs_db(connection, pairs, start_time, end_time)
                if df_agg.empty:
                    # 回退到Python端聚合
                    df_agg = aggregate_speed_data_for_schools(connection, remaining_schools, start_time, end_time)

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
                        save_results(results_remaining, out_remaining, True, args.direction, start_time, end_time, extra_log_prefix="[剩余组-汇总] ")
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
                        save_results(results_remaining, out_remaining, False, args.direction, start_time, end_time, extra_log_prefix="[剩余组-汇总] ")
            else:
                logger.warning("排除后无剩余院校可计算，跳过剩余组计算。")
        else:
            # 保持原有单组处理逻辑（逐校）
            results = process_schools(connection, schools, start_time, end_time, args.direction, args.export_daily)
            if args.export_daily:
                save_results(results, args.output, True, args.direction, start_time, end_time)
            else:
                save_results(results, args.output, False, args.direction, start_time, end_time)
                logger.info("汇总信息:")
                logger.info(f"  省份: {args.province}")
                logger.info(f"  CP类型: {args.cp}")
                if args.school:
                    logger.info(f"  指定院校: {args.school}")
                logger.info(f"  时间范围: {start_time} - {end_time}")
                logger.info(f"  流量方向: {args.direction}")
    
    except Exception as e:
        logger.error(f"处理过程中发生错误: {e}")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()
