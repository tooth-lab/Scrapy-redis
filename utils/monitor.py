import logging
import time
import redis
import json
from datetime import datetime
import threading
import psutil
import pymysql
from prettytable import PrettyTable
import os

class SpiderMonitor:
    def __init__(self, redis_host='localhost', redis_port=6379, mysql_config=None):
        # 设置日志
        self.setup_logging()
        
        # Redis连接
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        
        # MySQL配置
        self.mysql_config = mysql_config or {
            'host': 'localhost',
            'user': 'root',
            'password': '123456',
            'database': 'douban_spider'
        }
        
        # 状态统计
        self.stats = {
            'active_nodes': set(),
            'total_requests': 0,
            'success_requests': 0,
            'failed_requests': 0,
            'items_scraped': 0,
            'cpu_usage': 0,
            'memory_usage': 0
        }
        
        # 上次打印时间
        self.last_print_time = time.time()
        
    def setup_logging(self):
        """设置日志"""
        # 创建logs目录
        if not os.path.exists('logs'):
            os.makedirs('logs')
            
        # 设置日志
        self.logger = logging.getLogger('spider_monitor')
        self.logger.setLevel(logging.INFO)
        
        # 文件处理器
        fh = logging.FileHandler('logs/monitor.log', encoding='utf-8')
        fh.setLevel(logging.INFO)
        
        # 控制台处理器
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # 格式化器
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        # 添加处理器
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
    
    def _get_mysql_stats(self):
        """获取MySQL数据统计"""
        try:
            conn = pymysql.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            # 获取已爬取的电影数量
            cursor.execute("SELECT COUNT(*) FROM seed_urls WHERE status=1")
            crawled_count = cursor.fetchone()[0]
            
            # 获取失败的URL数量
            cursor.execute("SELECT COUNT(*) FROM seed_urls WHERE status=2")
            failed_count = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            self.logger.debug(f"MySQL统计: 已爬取={crawled_count}, 失败={failed_count}")
            return {
                'crawled_count': crawled_count,
                'failed_count': failed_count
            }
        except Exception as e:
            self.logger.error(f"获取MySQL统计信息失败: {e}")
            return {'crawled_count': 0, 'failed_count': 0}
    
    def _get_active_nodes(self):
        """获取活跃节点"""
        return set(["douban1","douban2","douban3"])
    
    def _get_system_stats(self):
        """获取系统资源使用情况"""
        stats = {
            'cpu_usage': psutil.cpu_percent(),
            'memory_usage': psutil.virtual_memory().percent
        }
        self.logger.debug(f"系统资源: CPU={stats['cpu_usage']}%, 内存={stats['memory_usage']}%")
        return stats
    
    def _update_stats(self):
        """更新统计信息"""
        try:
            # 更新活跃节点
            self.stats['active_nodes'] = self._get_active_nodes()
            
            # 更新MySQL统计
            mysql_stats = self._get_mysql_stats()
            self.stats.update(mysql_stats)
            
            # 更新系统资源使用情况
            system_stats = self._get_system_stats()
            self.stats.update(system_stats)
            
            self.logger.debug("统计信息更新完成")
            
        except Exception as e:
            self.logger.error(f"更新统计信息失败: {e}")
    
    def _print_stats(self):
        """打印统计信息"""
        table = PrettyTable()
        table.field_names = ["指标", "值"]
        table.align["指标"] = "l"
        table.align["值"] = "r"
        
        table.add_row(["活跃节点数", len(self.stats['active_nodes'])])
        table.add_row(["失败数量", self.stats.get('failed_count', 0)])
        table.add_row(["CPU使用率", f"{self.stats['cpu_usage']}%"])
        table.add_row(["内存使用率", f"{self.stats['memory_usage']}%"])
        
        status_str = "\n" + "="*50 + "\n"
        status_str += f"爬虫监控状态 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        status_str += str(table) + "\n"
        status_str += f"活跃节点: {', '.join(self.stats['active_nodes']) or '无'}\n"
        status_str += "="*50 + "\n"
        
        print(status_str)
        self.logger.info("\n" + status_str)
    
    def start(self):
        """启动监控"""
        self.logger.info("爬虫监控系统启动")
        
        try:
            while True:
                self._update_stats()
                
                # 每30秒打印一次统计信息
                if time.time() - self.last_print_time >= 30:
                    self._print_stats()
                    self.last_print_time = time.time()
                
                time.sleep(5)
                
        except KeyboardInterrupt:
            self.logger.info("监控系统停止")
        except Exception as e:
            self.logger.error(f"监控系统异常: {e}")

if __name__ == "__main__":
    monitor = SpiderMonitor()
    monitor.start()