#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
批量调整 nfa_ipgroup 表中 ipgroup_name 字段的工具
将老的命名规则调整为新的命名规则：院校名称_cp名称_V4/V6
其中cp名称使用mapping.json中的映射关系进行转换
只处理 type 为 "yuanxiao" 的记录
"""

import argparse
import pymysql
import sys
import logging
import configparser
import os
import json
from typing import Dict, List, Tuple

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("ipgroup_name更新工具")

class IPGroupNameUpdater:
    """
    工具类，用于批量调整 nfa_ipgroup 表中的 ipgroup_name 字段
    将老的命名规则调整为新的命名规则：院校名称_cp名称_V4/V6
    """
    
    def __init__(self, db_config: Dict, mapping_file: str, region: str = None, cp: str = None, 
                 schoolname: str = None, dry_run: bool = True):
        """
        初始化 ipgroup_name 更新工具
        
        Args:
            db_config: 数据库连接配置
            mapping_file: CP映射文件路径
            region: 可选，用于过滤的区域
            cp: 可选，用于过滤的CP类型
            schoolname: 可选，用于过滤的学校名称
            dry_run: 是否为演习模式（不实际更新数据库）
        """
        self.db_config = db_config
        self.mapping_file = mapping_file
        self.region = region
        self.cp = cp
        self.schoolname = schoolname
        self.dry_run = dry_run
        self.conn = None
        self.cursor = None
        self.cp_mapping = {}
        self.reverse_cp_mapping = {}
        
    def connect_db(self):
        """连接到MySQL数据库"""
        try:
            self.conn = pymysql.connect(
                host=self.db_config['host'],
                port=int(self.db_config['port']),
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['db'],
                charset=self.db_config['charset'],
                cursorclass=pymysql.cursors.DictCursor
            )
            self.cursor = self.conn.cursor()
            logger.info(f"成功连接到数据库 {self.db_config['db']}")
        except Exception as err:
            logger.error(f"数据库连接失败: {err}")
            sys.exit(1)
            
    def load_cp_mapping(self):
        """加载CP映射关系"""
        try:
            with open(self.mapping_file, 'r', encoding='utf-8') as f:
                self.cp_mapping = json.load(f)
                
            # 创建反向映射（从数据库值到显示名称）
            self.reverse_cp_mapping = {v: k for k, v in self.cp_mapping.items()}
            logger.info(f"成功加载CP映射关系，共 {len(self.cp_mapping)} 条")
        except Exception as err:
            logger.error(f"加载CP映射文件失败: {err}")
            sys.exit(1)
    
    def get_records_to_update(self) -> List[Dict]:
        """
        获取需要更新的记录
        
        Returns:
            需要更新的记录列表
        """
        try:
            # 构建查询条件
            conditions = ["type = 'yuanxiao'"]
            params = []
            
            if self.region:
                conditions.append("region = %s")
                params.append(self.region)
            
            if self.cp:
                conditions.append("cp = %s")
                params.append(self.cp)
            
            if self.schoolname:
                conditions.append("school_name = %s")
                params.append(self.schoolname)
            
            # 构建查询语句
            query = """
            SELECT id, ipgroup_id, ipgroup_name, cp, school_name, region, nfa_uuid, type
            FROM nfa_ipgroup
            """
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            self.cursor.execute(query, params)
            records = self.cursor.fetchall()
            logger.info(f"共获取 {len(records)} 条记录")
            return records
            
        except Exception as err:
            logger.error(f"查询记录失败: {err}")
            return []
    
    def analyze_records(self, records: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        分析记录，找出需要更新的记录
        
        Args:
            records: 从数据库获取的记录
            
        Returns:
            需要更新的记录列表和新命名规则的记录列表
        """
        records_to_update = []
        new_format_records = []
        
        # 按学校和CP分组
        school_cp_groups = {}
        
        for record in records:
            key = (record['school_name'], record['cp'])
            if key not in school_cp_groups:
                school_cp_groups[key] = []
            school_cp_groups[key].append(record)
        
        # 分析每个分组
        for (school_name, cp), group in school_cp_groups.items():
            # 获取CP的显示名称
            cp_display_name = self.reverse_cp_mapping.get(cp, cp)
            
            # 查找新格式的记录
            v4_new_format = None
            v6_new_format = None
            
            for record in group:
                ipgroup_name = record['ipgroup_name']
                parts = ipgroup_name.split('_')
                
                # 检查是否是新格式 (院校名称_cp名称_V4/V6)
                if len(parts) == 3 and (parts[2] == 'V4' or parts[2] == 'V6'):
                    if parts[2] == 'V4':
                        v4_new_format = record
                    else:
                        v6_new_format = record
                    new_format_records.append(record)
            
            # 处理旧格式的记录
            for record in group:
                ipgroup_name = record['ipgroup_name']
                parts = ipgroup_name.split('_')
                
                # 跳过已经是新格式的记录
                if len(parts) == 3 and (parts[2] == 'V4' or parts[2] == 'V6'):
                    continue
                
                # 检查是否包含V4/V6信息
                if 'V4' in ipgroup_name:
                    new_name = f"{school_name}_{cp_display_name}_V4"
                    record['new_ipgroup_name'] = new_name
                    records_to_update.append(record)
                elif 'V6' in ipgroup_name:
                    new_name = f"{school_name}_{cp_display_name}_V6"
                    record['new_ipgroup_name'] = new_name
                    records_to_update.append(record)
                else:
                    # 没有V4/V6信息的旧数据，统一改为V4
                    if v4_new_format:
                        new_name = f"{school_name}_{cp_display_name}_V4"
                        record['new_ipgroup_name'] = new_name
                        records_to_update.append(record)
        
        logger.info(f"发现 {len(new_format_records)} 条符合新命名规则的记录")
        logger.info(f"发现 {len(records_to_update)} 条需要更新的记录")
        return records_to_update, new_format_records
    
    def update_records(self, records_to_update: List[Dict]):
        """
        更新记录中的 ipgroup_name 字段
        
        Args:
            records_to_update: 需要更新的记录列表
        """
        if not records_to_update:
            logger.info("没有需要更新的记录")
            return
        
        try:
            update_query = """
            UPDATE nfa_ipgroup
            SET ipgroup_name = %s
            WHERE id = %s
            """
            
            updated_count = 0
            for record in records_to_update:
                old_name = record['ipgroup_name']
                new_name = record['new_ipgroup_name']
                
                if self.dry_run:
                    logger.info(f"[演习模式] 将更新记录 ID: {record['id']}, "
                              f"ipgroup_name从 '{old_name}' 更新为 '{new_name}'")
                else:
                    self.cursor.execute(update_query, (new_name, record['id']))
                    updated_count += 1
                    
                    # 每500条提交一次，避免事务过大
                    if updated_count % 500 == 0:
                        self.conn.commit()
                        logger.info(f"已更新 {updated_count} 条记录...")
            
            if not self.dry_run:
                self.conn.commit()
                logger.info(f"成功更新 {updated_count} 条记录")
            else:
                logger.info(f"[演习模式] 共有 {len(records_to_update)} 条记录需要更新")
                
        except Exception as err:
            logger.error(f"更新记录失败: {err}")
            if not self.dry_run:
                self.conn.rollback()
    
    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logger.info("数据库连接已关闭")
    
    def run(self):
        """
        运行 ipgroup_name 更新工具的主流程
        """
        self.connect_db()
        self.load_cp_mapping()
        
        records = self.get_records_to_update()
        records_to_update, new_format_records = self.analyze_records(records)
        
        self.update_records(records_to_update)
        self.close()
        
        return len(records_to_update)


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
        'port': config.get('DATABASE', 'port'),
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


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='批量调整 nfa_ipgroup 表中 ipgroup_name 字段的工具')
    parser.add_argument('--config', default='db_config.ini', help='数据库配置文件路径')
    parser.add_argument('--mapping', default='mapping.json', help='CP映射文件路径')
    parser.add_argument('--region', '-r', help='指定区域进行过滤，例如：山东')
    parser.add_argument('--cp', '-c', help='指定CP类型进行过滤，例如：bilibili')
    parser.add_argument('--school', '-s', help='指定学校名称进行过滤')
    parser.add_argument('--execute', action='store_true', help='实际执行更新操作，不加此参数则为演习模式')
    
    args = parser.parse_args()
    
    # 加载数据库配置
    db_config = load_db_config(args.config)
    
    # 创建并运行更新工具
    updater = IPGroupNameUpdater(
        db_config=db_config,
        mapping_file=args.mapping,
        region=args.region,
        cp=args.cp,
        schoolname=args.school,
        dry_run=not args.execute
    )
    
    updated_count = updater.run()
    
    if args.execute:
        logger.info(f"操作完成，共更新 {updated_count} 条记录")
    else:
        logger.info(f"演习模式完成，共有 {updated_count} 条记录需要更新")
        logger.info("如需实际执行更新，请添加 --execute 参数")


if __name__ == "__main__":
    main()
