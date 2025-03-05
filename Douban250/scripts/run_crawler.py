import os
import sys
import time
import argparse
import logging
from multiprocessing import Process
import socket
import json
import redis
from kafka import KafkaProducer
from datetime import datetime
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerProcess
from twisted.internet import reactor

# 获取项目根目录
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)

logger = logging.getLogger('crawler')

class CrawlerNode:
    def __init__(self, node_id, redis_host='localhost', redis_port=6379, kafka_servers='localhost:9092'):
        self.node_id = node_id
        self.hostname = socket.gethostname()
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.kafka_servers = kafka_servers
        
        # 初始化Redis连接
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
        
        # 初始化Kafka生产者
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except Exception as e:
            self.logger = self._setup_logger()
            self.logger.warning(f"Kafka连接失败，将不使用Kafka功能: {e}")
            self.kafka_producer = None
        
        # 设置日志
        self.logger = self._setup_logger()
        
    def _setup_logger(self):
        """设置日志"""
        # 创建logs目录（如果不存在）
        if not os.path.exists('logs'):
            os.makedirs('logs')
        
        # 设置节点特定的日志文件
        log_file = f'logs/crawler_{self.node_id}.log'
        spider_log_file = f'logs/spider_{self.node_id}.log'
        
        # 清空之前的日志文件
        open(log_file, 'w', encoding='utf-8').close()
        open(spider_log_file, 'w', encoding='utf-8').close()
        
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()  # 同时输出到控制台
            ]
        )
        
        self.logger = logging.getLogger(f'crawler_{self.node_id}')
        self.logger.setLevel(logging.INFO)
        return self.logger
        
    def register_node(self):
        """注册爬虫节点"""
        node_info = {
            'node_id': self.node_id,
            'hostname': self.hostname,
            'start_time': datetime.now().isoformat(),
            'status': 'active'
        }
        
        # 将节点信息存储到Redis
        self.redis_client.hset(
            'crawler_nodes',
            self.node_id,
            json.dumps(node_info)
        )
        
        self.logger.info(f"节点 {self.node_id} 注册成功")
        
    def heartbeat(self):
        """发送心跳"""
        while True:
            try:
                # 更新节点状态
                node_info = {
                    'node_id': self.node_id,
                    'hostname': self.hostname,
                    'last_heartbeat': datetime.now().isoformat(),
                    'status': 'active'
                }
                
                self.redis_client.hset(
                    'crawler_nodes',
                    self.node_id,
                    json.dumps(node_info)
                )
                
                # 发送状态到Kafka
                if self.kafka_producer:
                    self.kafka_producer.send(
                        'node_status',
                        {
                            'node_id': self.node_id,
                            'timestamp': datetime.now().isoformat(),
                            'status': 'active',
                            'memory_usage': 0,  # 这里可以添加更多状态信息
                            'cpu_usage': 0
                        }
                    )
                
                time.sleep(5)  # 每5秒发送一次心跳
                
            except Exception as e:
                self.logger.error(f"心跳发送失败: {e}")
                time.sleep(1)
                
    def start_crawler(self):
        """启动爬虫"""
        try:
            # 设置爬虫的命令行参数
            settings = get_project_settings()
            settings.set('LOG_FILE', f'logs/spider_{self.node_id}.log')  # 设置爬虫特定的日志文件
            settings.set('LOG_LEVEL', 'DEBUG')  # 设置为DEBUG级别
            settings.set('LOG_ENABLED', True)
            settings.set('REDIS_HOST', self.redis_host)
            settings.set('REDIS_PORT', self.redis_port)
            
            # 确保爬取250个电影
            settings.set('CLOSESPIDER_ITEMCOUNT', 250)
            
            # 创建爬虫进程
            process = CrawlerProcess(settings)
            # 使用固定的爬虫名称'douban2'
            process.crawl('douban2', 
                         node_id=self.node_id,
                         redis_host=self.redis_host,
                         redis_port=self.redis_port)
            
            self.logger.info(f"爬虫节点 {self.node_id} 开始运行")
            process.start()
            
        except Exception as e:
            self.logger.error(f"启动爬虫失败: {str(e)}")
            raise

def init_redis():
    """初始化Redis数据"""
    try:
        r = redis.Redis(host='localhost', port=6379, db=0)
        
        # 清空之前的数据
        for i in range(1, 4):
            node_id = f"douban{i}"
            r.delete(f'{node_id}:start_urls')
            r.delete(f'{node_id}:dupefilter')
            r.delete(f'{node_id}:requests')
        
        # 添加初始URL到每个节点
        urls = [
            {
                'url': 'https://movie.douban.com/top250',
                'meta': {
                    'page': 1,
                    'priority': 'high',
                    'rank_start': 1,
                    'rank_end': 25
                }
            },
            {
                'url': 'https://movie.douban.com/top250?start=25',
                'meta': {
                    'page': 2,
                    'priority': 'high',
                    'rank_start': 26,
                    'rank_end': 50
                }
            },
            {
                'url': 'https://movie.douban.com/top250?start=50',
                'meta': {
                    'page': 3,
                    'priority': 'medium',
                    'rank_start': 51,
                    'rank_end': 75
                }
            },
            {
                'url': 'https://movie.douban.com/top250?start=75',
                'meta': {
                    'page': 4,
                    'priority': 'medium',
                    'rank_start': 76,
                    'rank_end': 100
                }
            },
            {
                'url': 'https://movie.douban.com/top250?start=100',
                'meta': {
                    'page': 5,
                    'priority': 'medium',
                    'rank_start': 101,
                    'rank_end': 125
                }
            },
            {
                'url': 'https://movie.douban.com/top250?start=125',
                'meta': {
                    'page': 6,
                    'priority': 'medium',
                    'rank_start': 126,
                    'rank_end': 150
                }
            },
            {
                'url': 'https://movie.douban.com/top250?start=150',
                'meta': {
                    'page': 7,
                    'priority': 'low',
                    'rank_start': 151,
                    'rank_end': 175
                }
            },
            {
                'url': 'https://movie.douban.com/top250?start=175',
                'meta': {
                    'page': 8,
                    'priority': 'low',
                    'rank_start': 176,
                    'rank_end': 200
                }
            },
            {
                'url': 'https://movie.douban.com/top250?start=200',
                'meta': {
                    'page': 9,
                    'priority': 'low',
                    'rank_start': 201,
                    'rank_end': 225
                }
            },
            {
                'url': 'https://movie.douban.com/top250?start=225',
                'meta': {
                    'page': 10,
                    'priority': 'low',
                    'rank_start': 226,
                    'rank_end': 250
                }
            }
        ]
        
        for i in range(1, 4):
            node_id = f"douban{i}"
            for url_data in urls:
                r.lpush(f'{node_id}:start_urls', json.dumps(url_data))
            
        logger.info("Redis数据初始化完成")
        
    except Exception as e:
        logger.error(f"Redis初始化失败: {str(e)}")
        sys.exit(1)

def run_spider(node_id):
    """运行爬虫节点"""
    try:
        # 获取项目设置
        settings = get_project_settings()
        
        # 为每个节点设置不同的日志文件
        log_file = f'logs/spider_{node_id}.log'
        settings.set('LOG_FILE', log_file)
        
        # 创建日志目录
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # 创建爬虫进程
        process = CrawlerProcess(settings)
        
        # 添加爬虫，使用固定的爬虫名称'douban2'
        process.crawl('douban2', node_id=node_id)
        
        # 启动爬虫
        process.start()
        
    except Exception as e:
        logger.error(f"节点 {node_id} 运行失败: {str(e)}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='运行豆瓣电影爬虫')
    parser.add_argument('--node-id', type=str, help='节点ID')
    parser.add_argument('--init-redis', action='store_true', help='是否初始化Redis数据')
    parser.add_argument('--multi-node', type=int, help='启动多个节点（指定节点数量）')
    args = parser.parse_args()
    
    # 创建日志目录
    os.makedirs('logs', exist_ok=True)
    
    # 如果需要初始化Redis
    if args.init_redis:
        init_redis()
    
    # 多节点模式
    if args.multi_node:
        logger.info(f"准备启动 {args.multi_node} 个爬虫节点")
        processes = []
        
        for i in range(args.multi_node):
            node_id = f"douban{i+1}"  # 使用douban1、douban2、douban3作为节点ID
            p = Process(target=run_spider, args=(node_id,))
            processes.append(p)
            p.start()
            logger.info(f"节点 {node_id} 已启动")
            time.sleep(2)  # 间隔2秒启动下一个节点
        
        # 等待所有进程完成
        for p in processes:
            p.join()
            
        logger.info("所有节点已完成")
        
    # 单节点模式
    elif args.node_id:
        logger.info(f"启动单个节点: {args.node_id}")
        run_spider(args.node_id)
    
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 