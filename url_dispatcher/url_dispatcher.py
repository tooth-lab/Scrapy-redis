import logging
import time
from collections import defaultdict
from threading import Thread, Lock
from kafka import KafkaConsumer, KafkaProducer
import json
import redis
from datetime import datetime
import hashlib


class URLDispatcher:
    def __init__(self):
        self.logger = logging.getLogger('url_dispatcher')
        self.logger.setLevel(logging.INFO)

        # Kafka配置
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Redis配置
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)

        # 爬虫节点负载状态
        self.node_loads = defaultdict(int)
        self.node_loads_lock = Lock()

        # 域名访问频率控制
        self.domain_access_times = defaultdict(list)
        self.domain_access_lock = Lock()

        # 配置参数
        self.min_interval = 1  # 同一域名最小访问间隔(秒)
        self.max_load = 100  # 每个节点最大负载

        # 初始化各个组件
        self._init_kafka_consumer()
        self._init_priority_scheduler()

    def _init_kafka_consumer(self):
        """初始化Kafka消费者线程，用于处理节点状态更新"""

        def consume_status():
            consumer = KafkaConsumer(
                'node_status',
                bootstrap_servers='localhost:9092',
                group_id='dispatcher_group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )

            for message in consumer:
                try:
                    status = message.value
                    node_id = status['node_id']
                    completed_urls = status.get('completed_urls', 0)
                    failed_urls = status.get('failed_urls', [])

                    # 更新节点负载
                    self.update_node_load(node_id, -completed_urls)

                    # 重新分发失败的URL
                    for url in failed_urls:
                        self.dispatch_url(url, priority='high')  # 失败的URL使用高优先级重试
                except Exception as e:
                    self.logger.error(f"处理节点状态更新失败: {e}")

        # 启动状态监控线程
        status_monitor = Thread(target=consume_status)
        status_monitor.daemon = True
        status_monitor.start()

    def _init_priority_scheduler(self):
        """初始化优先级调度器"""

        def schedule_urls():
            while True:
                try:
                    # 优先处理高优先级URL
                    high_priority = self.redis_client.rpop('douban2:high_priority_urls')
                    if high_priority:
                        self.redis_client.lpush('douban2:start_urls', high_priority)
                        self.logger.info("移动高优先级URL到活跃队列")
                        continue

                    # 当主队列为空时，处理低优先级URL
                    if not self.redis_client.llen('douban2:start_urls'):
                        low_priority = self.redis_client.rpop('douban2:low_priority_urls')
                        if low_priority:
                            self.redis_client.lpush('douban2:start_urls', low_priority)
                            self.logger.info("移动低优先级URL到活跃队列")

                    time.sleep(1)  # 避免过于频繁的检查
                except Exception as e:
                    self.logger.error(f"优先级调度失败: {e}")
                    time.sleep(5)  # 出错后等待更长时间

        # 启动优先级调度器线程
        scheduler = Thread(target=schedule_urls)
        scheduler.daemon = True
        scheduler.start()
        self.logger.info("优先级调度器已启动")

    def get_domain(self, url):
        """从URL中提取域名"""
        from urllib.parse import urlparse
        return urlparse(url).netloc

    def calculate_node_for_domain(self, domain, available_nodes):
        """使用一致性哈希为域名选择节点"""
        if not available_nodes:
            return None

        hash_val = int(hashlib.md5(domain.encode()).hexdigest(), 16)
        return available_nodes[hash_val % len(available_nodes)]

    def can_access_domain(self, domain):
        """检查是否可以访问该域名"""
        with self.domain_access_lock:
            now = time.time()
            # 清理过期的访问记录
            self.domain_access_times[domain] = [t for t in self.domain_access_times[domain]
                                                if now - t < self.min_interval]

            if len(self.domain_access_times[domain]) >= 3:  # 最多允许3个并发请求
                return False

            self.domain_access_times[domain].append(now)
            return True

    def update_node_load(self, node_id, delta):
        """更新节点负载"""
        with self.node_loads_lock:
            self.node_loads[node_id] += delta

    def get_available_nodes(self):
        """获取可用的爬虫节点"""
        with self.node_loads_lock:
            return [node for node, load in self.node_loads.items()
                    if load < self.max_load]

    def dispatch_url(self, url, priority='medium', metadata=None):
        """分发URL到Kafka和Redis"""
        domain = self.get_domain(url)

        # 检查域名访问频率
        if not self.can_access_domain(domain):
            self.logger.info(f"域名 {domain} 访问过于频繁，URL {url} 将被延迟处理")
            return False

        # 检查URL是否已存在
        if self.redis_client.sismember('douban2:seen_urls', url):
            self.logger.info(f"URL {url} 已存在，跳过")
            return False

        # 获取可用节点
        available_nodes = self.get_available_nodes()
        if not available_nodes:
            self.logger.warning("没有可用的爬虫节点")
            return False

        # 选择目标节点
        target_node = self.calculate_node_for_domain(domain, available_nodes)

        # 构建消息
        message = {
            'url': url,
            'priority': priority,
            'timestamp': datetime.now().isoformat(),
            'target_node': target_node,
            'metadata': metadata or {}
        }

        try:
            # 发送到Kafka对应优先级的topic
            topic = f'spider_urls_{priority}'
            self.producer.send(topic, message)
            self.update_node_load(target_node, 1)
            self.logger.info(f"URL {url} 已分发到Kafka - 节点: {target_node}, 优先级: {priority}")

            # 添加到Redis相应的优先级队列
            message_json = json.dumps(message)
            if priority == 'high':
                self.redis_client.lpush('douban2:high_priority_urls', message_json)
                self.logger.info(f"URL {url} 已添加到高优先级队列")
            elif priority == 'low':
                self.redis_client.lpush('douban2:low_priority_urls', message_json)
                self.logger.info(f"URL {url} 已添加到低优先级队列")
            else:
                self.redis_client.lpush('douban2:start_urls', message_json)
                self.logger.info(f"URL {url} 已添加到普通优先级队列")

            # 添加到URL去重集合
            self.redis_client.sadd('douban2:seen_urls', url)

            return True
        except Exception as e:
            self.logger.error(f"URL分发失败: {e}")
            return False

    def run(self):
        """启动URL分发器"""
        self.logger.info("URL分发器启动...")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("URL分发器正在关闭...")
            self.producer.close()

    def close(self):
        """关闭分发器"""
        if hasattr(self, 'producer'):
            self.producer.close()
        self.logger.info("URL分发器已关闭")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    dispatcher = URLDispatcher()
    dispatcher.run()