#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import mysql.connector
import sys
from typing import Dict, List, Tuple


class CPCorrector:
    """
    工具类，用于检查并纠正数据表中的cp字段
    根据ipgroup_name中的y部分和提供的映射关系进行校验和修正
    """
    
    def __init__(self, db_config: Dict, mapping_file: str, nfa_uuid: str = None, dry_run: bool = True):
        """
        初始化CP校正工具
        
        Args:
            db_config: 数据库连接配置
            mapping_file: 包含y到cp映射关系的文件路径
            nfa_uuid: 可选，用于约束影响范围的nfa_uuid
            dry_run: 是否为演习模式（不实际更新数据库）
        """
        self.db_config = db_config
        self.mapping_file = mapping_file
        self.nfa_uuid = nfa_uuid
        self.dry_run = dry_run
        self.y_to_cp_map = {}
        self.conn = None
        self.cursor = None
        
    def connect_db(self):
        """连接到MySQL数据库"""
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor(dictionary=True)
            print(f"成功连接到数据库 {self.db_config['database']}")
        except mysql.connector.Error as err:
            print(f"数据库连接失败: {err}")
            sys.exit(1)
    
    def load_mapping(self):
        """加载y到cp的映射关系"""
        try:
            # 根据文件扩展名决定如何加载映射文件
            if self.mapping_file.endswith('.json'):
                with open(self.mapping_file, 'r', encoding='utf-8') as f:
                    self.y_to_cp_map = json.load(f)
            elif self.mapping_file.endswith('.csv'):
                with open(self.mapping_file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader, None)  # 跳过表头
                    for row in reader:
                        if len(row) >= 2:
                            self.y_to_cp_map[row[0]] = row[1]
            else:
                # 假设是简单的文本文件，每行一个映射关系
                with open(self.mapping_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        parts = line.strip().split(',')
                        if len(parts) >= 2:
                            self.y_to_cp_map[parts[0]] = parts[1]
            
            print(f"成功加载映射关系，共 {len(self.y_to_cp_map)} 条")
        except Exception as e:
            print(f"加载映射文件失败: {e}")
            sys.exit(1)
    
    def get_records_to_correct(self) -> List[Dict]:
        """
        获取需要纠正的记录
        
        Returns:
            需要纠正的记录列表
        """
        try:
            if self.nfa_uuid:
                query = """
                SELECT id, ipgroup_id, ipgroup_name, cp, nfa_uuid
                FROM nfa_ipgroup
                WHERE nfa_uuid = %s
                """
                self.cursor.execute(query, (self.nfa_uuid,))
                print(f"正在查询nfa_uuid为 {self.nfa_uuid} 的记录...")
            else:
                query = """
                SELECT id, ipgroup_id, ipgroup_name, cp, nfa_uuid
                FROM nfa_ipgroup
                """
                self.cursor.execute(query)
                print("正在查询所有记录...")
                
            records = self.cursor.fetchall()
            print(f"共获取 {len(records)} 条记录")
            return records
        except mysql.connector.Error as err:
            print(f"查询记录失败: {err}")
            return []
    
    def analyze_records(self, records: List[Dict]) -> Tuple[List[Dict], int]:
        """
        分析记录，找出需要纠正的记录
        
        Args:
            records: 从数据库获取的记录
            
        Returns:
            需要更新的记录列表和总数
        """
        records_to_update = []
        total_incorrect = 0
        
        for record in records:
            ipgroup_name = record['ipgroup_name']
            current_cp = record['cp']
            
            # 解析ipgroup_name，获取y部分
            parts = ipgroup_name.split('_')
            if len(parts) >= 2:
                y_part = parts[1]
                
                # 检查y部分是否在映射关系中
                if y_part in self.y_to_cp_map:
                    expected_cp = self.y_to_cp_map[y_part]
                    
                    # 如果当前cp值与预期不符
                    if current_cp != expected_cp:
                        total_incorrect += 1
                        record['expected_cp'] = expected_cp
                        records_to_update.append(record)
        
        print(f"发现 {total_incorrect} 条记录需要纠正")
        return records_to_update, total_incorrect
    
    def update_records(self, records_to_update: List[Dict]):
        """
        更新记录中的cp字段
        
        Args:
            records_to_update: 需要更新的记录列表
        """
        if not records_to_update:
            print("没有需要更新的记录")
            return
        
        try:
            update_query = """
            UPDATE nfa_ipgroup
            SET cp = %s
            WHERE id = %s
            """
            
            updated_count = 0
            for record in records_to_update:
                if self.dry_run:
                    print(f"[演习模式] 将更新记录 ID: {record['id']}, ipgroup_name: {record['ipgroup_name']}, "
                          f"CP从 '{record['cp']}' 更新为 '{record['expected_cp']}'")
                else:
                    self.cursor.execute(update_query, (record['expected_cp'], record['id']))
                    updated_count += 1
                    
                    # 每500条提交一次，避免事务过大
                    if updated_count % 500 == 0:
                        self.conn.commit()
                        print(f"已更新 {updated_count} 条记录...")
            
            if not self.dry_run:
                self.conn.commit()
                print(f"成功更新 {updated_count} 条记录")
            else:
                print(f"[演习模式] 共有 {len(records_to_update)} 条记录需要更新")
                
        except mysql.connector.Error as err:
            print(f"更新记录失败: {err}")
            if not self.dry_run:
                self.conn.rollback()
    
    def export_corrections(self, records_to_update: List[Dict], output_file: str):
        """
        将需要纠正的记录导出到文件
        
        Args:
            records_to_update: 需要更新的记录
            output_file: 输出文件路径
        """
        if not records_to_update:
            print("没有需要导出的记录")
            return
            
        try:
            with open(output_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['id', 'ipgroup_id', 'ipgroup_name', 'current_cp', 'expected_cp', 'nfa_uuid'])
                
                for record in records_to_update:
                    writer.writerow([
                        record['id'],
                        record['ipgroup_id'],
                        record['ipgroup_name'],
                        record['cp'],
                        record['expected_cp'],
                        record.get('nfa_uuid', '')
                    ])
            
            print(f"已将 {len(records_to_update)} 条需要纠正的记录导出到 {output_file}")
        except Exception as e:
            print(f"导出记录失败: {e}")
    
    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("数据库连接已关闭")
    
    def run(self, export_file=None):
        """
        运行CP校正工具的主流程
        
        Args:
            export_file: 可选的导出文件路径
        """
        self.connect_db()
        self.load_mapping()
        
        records = self.get_records_to_correct()
        records_to_update, total_incorrect = self.analyze_records(records)
        
        if export_file and records_to_update:
            self.export_corrections(records_to_update, export_file)
        
        self.update_records(records_to_update)
        self.close()
        
        return total_incorrect


def main():
    parser = argparse.ArgumentParser(description='CP字段校正工具')
    parser.add_argument('--host', default='localhost', help='数据库主机')
    parser.add_argument('--port', type=int, default=3306, help='数据库端口')
    parser.add_argument('--user', required=True, help='数据库用户名')
    parser.add_argument('--password', required=True, help='数据库密码')
    parser.add_argument('--database', required=True, help='数据库名')
    parser.add_argument('--mapping', required=True, help='y到cp的映射文件路径')
    parser.add_argument('--nfa-uuid', help='约束影响范围的nfa_uuid')
    parser.add_argument('--export', help='导出需要纠正的记录到指定文件')
    parser.add_argument('--execute', action='store_true', help='实际执行更新操作，不加此参数则为演习模式')
    
    args = parser.parse_args()
    
    db_config = {
        'host': args.host,
        'port': args.port,
        'user': args.user,
        'password': args.password,
        'database': args.database
    }
    
    corrector = CPCorrector(db_config, args.mapping, args.nfa_uuid, not args.execute)
    corrector.run(args.export)


if __name__ == "__main__":
    main()
