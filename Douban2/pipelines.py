# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy import signals
from scrapy.exceptions import DropItem
import pandas as pd
import logging
import os
import sys
from datetime import datetime
import json
import sqlite3
import traceback  # 添加在文件开头的导入部分

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入数据存储模块
from utils.data_storage import get_data_storage


class Douban2Pipeline:
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        # 注册spider_idle信号处理器
        crawler.signals.connect(pipeline.spider_idle, signal=signals.spider_idle)
        return pipeline

    def __init__(self):
        self.items = []
        self.file = None
        self.logger = logging.getLogger(__name__)
        self.idle_count = 0  # 添加空闲计数器
        self.data_saved = False
        self.is_closing = False  # 添加关闭标志
        # 清理日志文件
        try:
            log_dir = 'logs'
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            
            # 清空所有节点的日志文件
            for i in range(1, 4):  # 针对douban1、douban2、douban3
                log_file = os.path.join(log_dir, f'spider_douban{i}.log')
                with open(log_file, 'w', encoding='utf-8') as f:
                    f.write(f'=== 日志开始于 {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ===\n')
                self.logger.info(f"已清空日志文件: {log_file}")
        except Exception as e:
            self.logger.error(f"清理日志文件失败: {str(e)}")
        
        # 初始化Redis连接
        try:
            import redis
            self.redis_conn = redis.Redis(
                host='localhost',
                port=6379,
                db=0,
                decode_responses=True
            )
            self.logger.info("Redis连接初始化成功")
        except Exception as e:
            self.logger.error(f"Redis连接初始化失败: {str(e)}")
            self.redis_conn = None
        
        # 初始化数据存储
        try:
            self.data_storage = get_data_storage()
            self.logger.info("数据存储模块初始化成功")
        except Exception as e:
            self.logger.error(f"数据存储模块初始化失败: {str(e)}")
            self.data_storage = None

    def get_active_nodes(self):
        """获取活跃节点数量"""
        try:
            # 统一使用 movies_douban 前缀
            pattern = "movies_douban*"
            active_nodes = len(self.redis_conn.keys(pattern))
            # 如果没有检测到节点，至少返回1
            return max(active_nodes, 1)
        except Exception as e:
            self.logger.error(f"获取活跃节点数量失败: {str(e)}")
            return 1

    def register_node(self, spider):
        """注册当前节点"""
        try:
            node_id = getattr(spider, 'node_id', 'unknown')
            self.redis_conn.sadd("active_nodes", node_id)
            self.redis_conn.expire("active_nodes", 300)  # 5分钟过期
        except Exception as e:
            self.logger.error(f"注册节点失败: {str(e)}")

    def get_total_items(self):
        """获取所有节点的总采集数量"""
        try:
            total = 0
            pattern = "movies_douban[1-3]"  # 只匹配节点特定的数据
            # 获取所有节点的数据
            for key in self.redis_conn.keys(pattern):
                # 获取该节点的所有电影数据
                movies = self.redis_conn.hgetall(key)
                # 计算该节点的电影数量
                total += len(movies)
            self.logger.info(f"当前总采集数量: {total}")
            return total
        except Exception as e:
            self.logger.error(f"获取总采集数量失败: {str(e)}")
            return len(self.items)

    def calculate_node_limit(self, spider):
        """计算当前节点的采集上限"""
        try:
            # 获取当前节点的序号（从节点ID中提取）
            node_id = getattr(spider, 'node_id', 'douban1')
            node_num = int(node_id.replace('douban', ''))
            if not 1 <= node_num <= 3:
                raise ValueError(f"节点序号 {node_num} 不在有效范围内(1-3)")
                
            # 固定分配范围 - 总共250部电影
            if node_num == 1:
                return 84  # 1-84 (84部)
            elif node_num == 2:
                return 83  # 85-167 (83部)
            else:
                return 83  # 168-250 (83部)
                
        except Exception as e:
            self.logger.error(f"解析节点序号失败: {str(e)}")
            return 84  # 默认配额

    def process_item(self, item, spider):
        if not item.get('name') or not item.get('rank'):
            raise DropItem("缺少必要字段")
            
        try:
            # 重置空闲计数器
            self.idle_count = 0
            
            # 数据格式化
            item['rank'] = int(item['rank'])
            
            if item['rank'] > 250:
                raise DropItem(f"跳过排名 {item['rank']} 的电影，超出了1-250的范围")
                
            if 'rate' in item:
                item['rate'] = float(item['rate'])
            if 'num' in item:
                item['num'] = int(item['num'].replace('人评价', ''))
                
            # 添加爬取时间和节点信息
            processed_item = dict(item)
            processed_item['crawl_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            processed_item['node_id'] = getattr(spider, 'node_id', 'unknown')  # 使用node_id而不是name
            
            # 确保所有字段都是简单类型
            for key, value in processed_item.items():
                if isinstance(value, (list, dict)):
                    processed_item[key] = json.dumps(value, ensure_ascii=False)
            
            # 将数据存入Redis的两个位置
            try:
                # 1. 存入节点特定的hash，统一使用 movies_douban 前缀
                node_key = f"movies_{processed_item['node_id']}"  # 使用node_id
                movie_json = json.dumps(processed_item, ensure_ascii=False)
                self.redis_conn.hset(
                    node_key,
                    str(processed_item['rank']),
                    movie_json
                )
                spider.logger.debug(f"已存入节点数据: {node_key}, rank={processed_item['rank']}")
                
                # 2. 存入全局电影集合
                all_movies_key = "all_movies"
                self.redis_conn.hset(
                    all_movies_key,
                    str(processed_item['rank']),
                    movie_json
                )
                spider.logger.debug(f"已存入全局数据: rank={processed_item['rank']}")
                
                # 3. 打印Redis存储状态
                node_count = len(self.redis_conn.hgetall(node_key))
                all_count = len(self.redis_conn.hgetall(all_movies_key))
                spider.logger.info(f"Redis存储状态 - 节点{node_key}: {node_count}条, 全局: {all_count}条")
                
            except Exception as e:
                self.logger.error(f"Redis存储失败: {str(e)}")
                
            self.items.append(processed_item)
            
            # 获取最新统计信息
            total_items = self.get_total_items()
            # 详细的日志信息
            spider.logger.info(
                f"采集进度 - 排名: {item['rank']}, "
                f"当前节点: {len(self.items)}, "
                f"总进度: {total_items}/250"
            )
            
            return item
        except Exception as e:
            self.logger.error(f"数据处理错误: {str(e)}")
            raise DropItem(f"数据处理错误: {str(e)}")

    def spider_idle(self, spider):
        """处理爬虫空闲状态"""
        try:
            # 获取Redis中剩余的请求数量
            remaining_requests = len(spider.server.keys(f"{spider.node_id}:requests*")) if hasattr(spider, 'server') else 0
            
            # 获取当前活跃节点数
            active_count = self.get_active_nodes()
            
            self.idle_count += 1
            spider.logger.info(f"爬虫空闲次数: {self.idle_count}")
            spider.logger.info(f"Redis中剩余请求数: {remaining_requests}")
            spider.logger.info(f"当前活跃节点数: {active_count}")
            
            # 检查当前节点的数据量
            node_items = len(self.items)
            spider.logger.info(f"当前节点已采集数据量: {node_items}")
            
            # 获取节点配额
            node_limit = self.calculate_node_limit(spider)
            spider.logger.info(f"当前节点配额: {node_limit}")
            
            # 判断是否应该关闭爬虫
            should_close = False
            
            # 情况1: 达到节点配额
            if node_items >= node_limit:
                spider.logger.info("准备关闭爬虫，原因: 达到节点配额")
                should_close = True
            
            # 情况2: 持续空闲且无剩余任务
            elif self.idle_count >= 5 and remaining_requests == 0:
                spider.logger.info("准备关闭爬虫，原因: 持续空闲且无剩余任务")
                should_close = True
            
            # 如果还有请求，重置空闲计数器
            if remaining_requests > 0:
                self.idle_count = 0
                spider.logger.info("检测到剩余请求，重置空闲计数器")
            
            if should_close:
                spider.logger.info("开始保存数据...")
                self.save_data(spider)
                return True
                
            return False
            
        except Exception as e:
            spider.logger.error(f"spider_idle处理出错: {str(e)}")
            return False

    def save_data(self, spider):
        """保存数据到文件"""
        try:
            # 如果数据已保存或正在关闭，直接返回
            if self.data_saved or self.is_closing:
                self.logger.info("数据已保存或正在关闭，跳过重复保存")
                return
                
            # 设置关闭标志
            self.is_closing = True
            
            # 获取当前节点数据
            node_id = getattr(spider, 'node_id', 'unknown')
            
            # 检查节点名称是否合法
            if not node_id or not node_id.startswith('douban') or not node_id[-1].isdigit():
                self.logger.error(f"节点ID不合法: {node_id}，应该是douban1、douban2或douban3")
                return
                
            self.logger.info(f"开始保存数据 - 节点: {node_id}, 当前节点数据量: {len(self.items)}")
            
            # 如果当前节点有数据则保存节点数据
            if self.items:
                try:
                    # 转换为DataFrame
                    df = pd.DataFrame(self.items)
                    
                    # 保存节点数据到SQLite
                    conn = sqlite3.connect('output/movies.db')
                    table_name = f'movies_{node_id}'  # 使用node_id作为表名
                    df.to_sql(table_name, conn, if_exists='replace', index=False)
                    self.logger.info(f"已保存节点数据到SQLite表: {table_name}")
                    conn.close()
                except Exception as e:
                    self.logger.error(f"保存节点数据失败: {str(e)}")
            
            # 检查是否所有节点都已完成
            try:
                # 获取所有节点的数据量
                node_data = {}
                for i in range(1, 4):
                    key = f"movies_douban{i}"
                    data = self.redis_conn.hgetall(key)
                    node_data[key] = len(data)
                    
                total_items = sum(node_data.values())
                self.logger.info(f"节点数据统计: {node_data}, 总数据量: {total_items}")
                
                # 只在最后一个节点完成时生成汇总（通过检查Redis中的数据总量）
                if total_items >= 50:
                    self.logger.info("所有节点数据采集完成，开始生成汇总")
                    
                    # 创建输出目录
                    os.makedirs('output', exist_ok=True)  # 确保输出目录存在
                    os.makedirs('output/parquet', exist_ok=True)  # 确保parquet目录存在
                    
                    # 获取时间戳
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    
                    # 从SQLite读取所有节点的数据
                    conn = sqlite3.connect('output/movies.db')
                    cursor = conn.cursor()
                    
                    # 先删除已存在的视图和表（注意顺序）
                    try:
                        # 先检查视图是否存在
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='view' AND name='movies_summary'")
                        if cursor.fetchone():
                            cursor.execute('DROP VIEW movies_summary')
                            self.logger.info("已删除旧的movies_summary视图")
                        
                        # 再检查表是否存在
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='movies_all'")
                        if cursor.fetchone():
                            cursor.execute('DROP TABLE movies_all')
                            self.logger.info("已删除旧的movies_all表")
                        
                        conn.commit()
                    except Exception as e:
                        self.logger.error(f"删除旧视图/表失败: {str(e)}")
                        self.logger.error(traceback.format_exc())
                    
                    # 读取每个节点的数据
                    all_data = []
                    for i in range(1, 4):
                        table_name = f'movies_douban{i}'
                        try:
                            # 先检查表是否存在
                            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                            if cursor.fetchone():
                                df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
                                all_data.append(df)
                                self.logger.info(f"成功读取表 {table_name}，数据量: {len(df)}")
                            else:
                                self.logger.warning(f"表 {table_name} 不存在")
                        except Exception as e:
                            self.logger.error(f"读取表 {table_name} 失败: {str(e)}")
                    
                    if all_data:
                        try:
                            # 合并所有数据
                            all_df = pd.concat(all_data, ignore_index=True)
                            all_df = all_df.drop_duplicates(subset=['rank'])
                            all_df = all_df.sort_values('rank')
                            self.logger.info(f"合并后的总数据量: {len(all_df)}")
                            
                            # 保存汇总Excel
                            summary_excel_path = f'output/豆瓣Top250_汇总_{timestamp}.xlsx'
                            all_df.to_excel(summary_excel_path, index=False, engine='openpyxl')
                            self.logger.info(f"已保存汇总Excel文件: {summary_excel_path}")
                            
                            # 保存汇总表
                            all_df.to_sql('movies_all', conn, if_exists='replace', index=False)
                            self.logger.info("已保存汇总数据到SQLite表: movies_all")
                            
                            # 创建汇总视图
                            cursor.execute("""
                            CREATE VIEW movies_summary AS
                            SELECT 
                                name,
                                rank,
                                rate,
                                num,
                                crawl_time,
                                node_id
                            FROM movies_all
                            ORDER BY rank
                            """)
                            conn.commit()
                            self.logger.info("已创建SQLite视图: movies_summary")
                            
                            # 保存汇总Parquet
                            summary_parquet_path = f'output/parquet/movies_all_{timestamp}.parquet'
                            all_df.to_parquet(summary_parquet_path, index=False)
                            self.logger.info(f"已保存汇总Parquet文件: {summary_parquet_path}")
                            
                            # 更新统计信息
                            with open('output/crawl_summary.txt', 'w', encoding='utf-8') as f:
                                f.write(f"采集时间: {timestamp}\n")
                                f.write(f"总数据量: {len(all_df)}\n")
                                f.write(f"排名范围: {all_df['rank'].min()} - {all_df['rank'].max()}\n")
                                f.write(f"参与节点: {', '.join(all_df['node_id'].unique())}\n")
                                f.write("\n节点数据统计:\n")
                                for node, count in node_data.items():
                                    f.write(f"{node}: {count}条\n")
                            self.logger.info("已更新汇总统计信息: output/crawl_summary.txt")
                            
                        except Exception as e:
                            self.logger.error(f"保存汇总文件失败: {str(e)}")
                            self.logger.error(traceback.format_exc())
                    
                    conn.close()
                    
            except Exception as e:
                self.logger.error(f"生成汇总数据失败: {str(e)}")
                self.logger.error(f"详细错误: {traceback.format_exc()}")
            
            # 重置items列表
            self.items = []
            
            # 标记数据已保存
            self.data_saved = True
            
        except Exception as e:
            self.logger.error(f"保存数据时发生错误: {str(e)}")
            self.logger.error(f"详细错误: {traceback.format_exc()}")
            
        finally:
            # 确保爬虫能够正常关闭
            if hasattr(spider, 'crawler'):
                spider.crawler.engine.close_spider(spider, '数据保存完成，关闭爬虫')

    def close_spider(self, spider):
        """关闭爬虫时的处理"""
        try:
            if not self.data_saved and not self.is_closing:
                self.save_data(spider)
        except Exception as e:
            self.logger.error(f"关闭爬虫时保存数据失败: {str(e)}")
        finally:
            # 清理Redis连接
            if hasattr(self, 'redis_conn'):
                self.redis_conn.close()
            spider.logger.info("爬虫已关闭")