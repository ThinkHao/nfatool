#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
limitations表 hash_uuid 批量更新脚本
遍历limitations表中type为yuanxiao的记录，
用其hash_uuid在nfa_ipgroup表查到region、cp、school_name，
再用这三个字段去nfa_ipgroup查最新的hash_uuid，
如有不同则更新（支持dry-run）。
"""
import argparse
import pymysql
import configparser
import os
import sys
import logging
from datetime import datetime

# 设置日志级别为INFO，确保所有提示都能显示
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')

def load_db_config(config_file):
    if not os.path.exists(config_file):
        print(f"配置文件 {config_file} 不存在，请先创建！")
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

def connect_db(cfg):
    return pymysql.connect(**cfg)

def main():
    parser = argparse.ArgumentParser(description='批量更新limitations表中的hash_uuid字段')
    parser.add_argument('--config', default='db_config.ini', help='数据库配置文件路径')
    parser.add_argument('--dry-run', action='store_true', help='仅预览将要执行的修改，不实际更新数据库')
    parser.add_argument('--exclude-region', default=None, help='指定region跳过不处理，如四川省')
    parser.add_argument('--include-region', default=None, help='只处理指定region，如湖北省')
    args = parser.parse_args()

    cfg = load_db_config(args.config)
    conn = connect_db(cfg)
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    # 1. 读取limitations表中所有type为yuanxiao的记录
    cursor.execute('''
        SELECT l.id, l.hash_uuid, l.name
        FROM limitations l
        WHERE l.hash_uuid IS NOT NULL
    ''')
    limitations = cursor.fetchall()
    update_count = 0
    preview_rows = []

    for row in limitations:
        lim_id = row['id']
        old_hash_uuid = row['hash_uuid']
        # 先用hash_uuid查nfa_ipgroup
        cursor.execute('''
            SELECT region, cp, school_name, type
            FROM nfa_ipgroup WHERE hash_uuid=%s
        ''', (old_hash_uuid,))
        ipgroup = cursor.fetchone()
        if not ipgroup:
            logging.warning(f'id={lim_id} hash_uuid={old_hash_uuid} 在nfa_ipgroup表未找到，跳过')
            continue
        if ipgroup['type'] != 'yuanxiao':
            continue
        region, cp, school_name = ipgroup['region'], ipgroup['cp'], ipgroup['school_name']
        # region过滤
        if args.include_region and region != args.include_region:
            logging.info(f'id={lim_id} region={region} 不在处理范围（只处理{args.include_region}），跳过')
            continue
        if args.exclude_region and region == args.exclude_region:
            logging.info(f'id={lim_id} region={region} 被排除，跳过')
            continue
        name = row['name'] or ''
        # 查询所有匹配region/cp/school_name的记录，带上ipgroup_name
        cursor.execute('''
            SELECT hash_uuid, create_time, ipgroup_name
            FROM nfa_ipgroup
            WHERE region=%s AND cp=%s AND school_name=%s AND type='yuanxiao' AND hash_uuid IS NOT NULL
            ORDER BY create_time DESC
        ''', (region, cp, school_name))
        candidates = cursor.fetchall()
        import re
        def extract_suffix(s):
            # 匹配如“V4-1”、“V6-2”等后缀，返回如V4-1，否则返回空串
            m = re.search(r'(V[46][-—][12])', s or '', re.IGNORECASE)
            return m.group(1) if m else ''
        def filter_by_keywords(candidates, keywords):
            for kw in keywords:
                filtered = [c for c in candidates if kw in (c['ipgroup_name'] or '')]
                if filtered:
                    return filtered
            return []
        # 新增：优先严格匹配V4-1/V4-2等后缀
        suffix = extract_suffix(name)
        strict_candidates = []
        if suffix:
            strict_candidates = [c for c in candidates if suffix in (c['ipgroup_name'] or '')]
        if strict_candidates:
            new_hash_uuid = strict_candidates[0]['hash_uuid']
        else:
            # 规则1：name含v4/v6/V4/V6，则优先ipgroup_name含相同的
            keywords = []
            if any(x in name for x in ['v4','V4','v6','V6']):
                for x in ['v4','V4','v6','V6']:
                    if x in name:
                        keywords.append(x)
                preferred = filter_by_keywords(candidates, keywords)
                if preferred:
                    new_hash_uuid = preferred[0]['hash_uuid']
                else:
                    new_hash_uuid = candidates[0]['hash_uuid'] if candidates else old_hash_uuid
            else:
                # 规则2：优先ipgroup_name含v4/V4
                preferred = filter_by_keywords(candidates, ['v4','V4'])
                if preferred:
                    new_hash_uuid = preferred[0]['hash_uuid']
                else:
                    new_hash_uuid = candidates[0]['hash_uuid'] if candidates else old_hash_uuid
        if not candidates:
            logging.warning(f'id={lim_id} {region}/{cp}/{school_name} 未找到可用hash_uuid，跳过')
            continue

        if new_hash_uuid != old_hash_uuid:
            # 检查目标hash_uuid是否已存在于limitations表（排除自身）
            cursor.execute('SELECT id, name FROM limitations WHERE hash_uuid=%s AND id!=%s', (new_hash_uuid, lim_id))
            conflict = cursor.fetchone()
            if conflict:
                logging.error(f"跳过id={lim_id}，因目标hash_uuid={new_hash_uuid}已被id={conflict['id']} name={conflict['name']}占用")
                continue
            # 获取old/new的ipgroup_name
            old_ipgroup_name = None
            new_ipgroup_name = None
            for c in candidates:
                if c['hash_uuid'] == old_hash_uuid:
                    old_ipgroup_name = c['ipgroup_name']
                if c['hash_uuid'] == new_hash_uuid:
                    new_ipgroup_name = c['ipgroup_name']
            preview_rows.append({
                'id': lim_id,
                'name': row['name'],
                'old_hash_uuid': old_hash_uuid,
                'new_hash_uuid': new_hash_uuid,
                'old_ipgroup_name': old_ipgroup_name,
                'new_ipgroup_name': new_ipgroup_name,
                'check_status_change': f"nfa_ipgroup: {old_hash_uuid} -> check_status=0, {new_hash_uuid} -> check_status=1"
            })
            if not args.dry_run:
                cursor.execute('UPDATE limitations SET hash_uuid=%s, updated_at=%s WHERE id=%s',
                               (new_hash_uuid, datetime.now(), lim_id))
                # 新增：同步check_status
                cursor.execute('UPDATE nfa_ipgroup SET check_status=0 WHERE hash_uuid=%s', (old_hash_uuid,))
                cursor.execute('UPDATE nfa_ipgroup SET check_status=1 WHERE hash_uuid=%s', (new_hash_uuid,))
                update_count += 1
    if preview_rows:
        print(f"将要更新 {len(preview_rows)} 条记录:")
        for r in preview_rows:
            print(f"id={r['id']} name={r['name']}\n  old: {r['old_hash_uuid']} [{r['old_ipgroup_name']}]\n  new: {r['new_hash_uuid']} [{r['new_ipgroup_name']}]\n  {r['check_status_change']}\n")
    else:
        print("没有需要更新的记录")
    if not args.dry_run and preview_rows:
        conn.commit()
        print(f"已实际更新 {update_count} 条记录")
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()
