from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
import json
import logging
import time

class KafkaURLManager:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # 确保消息可靠投递
            retries=5,  # 增加重试次数
            request_timeout_ms=30000,  # 增加请求超时时间
            connections_max_idle_ms=300000  # 增加连接空闲时间
        )
        # 分别为种子URL和详情页URL设置topic
        self.priority_topics = {
            'seed': {
                'high': 'douban_seed_high',     # 排名1-100的列表页
                'medium': 'douban_seed_medium',  # 排名101-200的列表页
                'low': 'douban_seed_low'        # 排名201-250的列表页
            },
            'detail': {
                'high': 'douban_detail_high',    # 排名1-100的详情页
                'medium': 'douban_detail_medium', # 排名101-200的详情页
                'low': 'douban_detail_low'       # 排名201-250的详情页
            }
        }
        self._ensure_topics_exist()
        
    def _ensure_topics_exist(self):
        """确保所有需要的topic都存在"""
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            existing_topics = admin_client.list_topics()
            
            new_topics = []
            for type_topics in self.priority_topics.values():
                for topic in type_topics.values():
                    if topic not in existing_topics:
                        new_topics.append(NewTopic(
                            name=topic,
                            num_partitions=3,
                            replication_factor=1
                        ))
            
            if new_topics:
                admin_client.create_topics(new_topics)
        except Exception as e:
            logging.error(f"创建Kafka topics失败: {e}")
            
    def _ensure_topic_exists(self, topic_name):
        """确保指定的topic存在"""
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            existing_topics = admin_client.list_topics()
            
            if topic_name not in existing_topics:
                new_topic = NewTopic(
                    name=topic_name,
                    num_partitions=3,
                    replication_factor=1
                )
                admin_client.create_topics([new_topic])
                logging.info(f"创建Kafka topic: {topic_name}")
            
            return True
        except Exception as e:
            logging.error(f"确保Kafka topic存在失败: {e}")
            return False
            
    def get_priority_topic(self, url_type, rank):
        """根据URL类型和排名确定优先级topic"""
        if rank <= 100:
            return self.priority_topics[url_type]['high']
        elif rank <= 200:
            return self.priority_topics[url_type]['medium']
        else:
            return self.priority_topics[url_type]['low']
            
    def send_url(self, url, rank=None, metadata=None, priority='medium'):
        """发送URL到Kafka"""
        try:
            # 确定主题名称
            topic_name = f'spider_urls_{priority}'
            
            # 确保主题存在
            self._ensure_topic_exists(topic_name)
            
            # 准备消息数据
            data = {
                'url': url,
                'rank': rank,
                'timestamp': int(time.time()),
                'metadata': metadata or {}
            }
            
            # 发送消息
            self.producer.send(
                topic_name,
                value=data  # 不需要手动编码，producer的value_serializer会处理
            )
            self.producer.flush()  # 确保消息被发送
            
            logging.info(f"URL已发送到Kafka: {url} (主题: {topic_name})")
            return True
        except Exception as e:
            logging.error(f"发送URL到Kafka失败: {e}")
            return False

    def get_urls_to_crawl(self, url_type='seed', priority='high', batch_size=10):
        """从Kafka获取待爬取的URL"""
        try:
            topic_name = f'douban_{url_type}_{priority}'
            group_id = f'douban_spider_{url_type}_{priority}'
            
            logging.info(f"尝试从主题 {topic_name} 获取URL (消费者组: {group_id})")
            
            # 确保主题存在
            if not self._ensure_topic_exists(topic_name):
                logging.warning(f"主题 {topic_name} 不存在")
                return []
            
            # 创建消费者
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=self.bootstrap_servers,
                group_id=group_id,
                auto_offset_reset='earliest',  # 从最早的消息开始消费
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=5000  # 5秒超时
            )
            
            # 获取消息并去重
            urls = []
            seen_urls = set()  # 用于去重
            
            for message in consumer:
                try:
                    data = message.value
                    url = data.get('url')
                    
                    # 去重
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        urls.append(data)
                        logging.info(f"从Kafka获取到URL: {url}")
                        
                        if len(urls) >= batch_size:
                            break
                except Exception as e:
                    logging.error(f"处理Kafka消息失败: {e}")
            
            # 关闭消费者
            consumer.close()
            
            logging.info(f"从主题 {topic_name} 获取到 {len(urls)} 个URL")
            return urls
        except Exception as e:
            logging.error(f"从Kafka获取URL失败: {e}")
            return []

    def send_detail_url(self, url, rank, movie_name, metadata=None):
        """发送详情页URL到对应优先级的队列"""
        try:
            topic = self.get_priority_topic('detail', rank)
            message = {
                'url': url,
                'rank': rank,
                'movie_name': movie_name,
                'retry_count': 0,
                'metadata': metadata or {}
            }
            future = self.producer.send(topic, value=message)
            future.get(timeout=10)
            return True
        except Exception as e:
            logging.error(f"发送详情页URL到Kafka失败: {e}")
            return False

    def reset_topic_offset(self, url_type, priority):
        """重置指定topic的消费位置"""
        try:
            topic = self.priority_topics[url_type][priority]
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=f'douban_spider_{url_type}_{priority}'
            )
            
            # 获取topic的所有分区
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                return False
            
            # 重置每个分区的offset
            for partition in partitions:
                tp = TopicPartition(topic, partition)
                consumer.assign([tp])
                consumer.seek_to_beginning(tp)
            
            consumer.close()
            return True
        except Exception as e:
            logging.error(f"重置topic offset失败: {e}")
            return False

    def get_topic_stats(self, url_type=None, priority=None):
        """获取topic统计信息"""
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers)
            
            stats = {}
            topics = []
            
            if url_type and priority:
                topics.append(self.priority_topics[url_type][priority])
            elif url_type:
                topics.extend(self.priority_topics[url_type].values())
            else:
                for type_topics in self.priority_topics.values():
                    topics.extend(type_topics.values())
            
            for topic in topics:
                partitions = consumer.partitions_for_topic(topic)
                if not partitions:
                    continue
                    
                topic_stats = {
                    'partitions': len(partitions),
                    'messages': 0,
                    'earliest_offset': float('inf'),
                    'latest_offset': 0
                }
                
                for partition in partitions:
                    tp = TopicPartition(topic, partition)
                    # 获取最早和最新offset
                    earliest = consumer.beginning_offsets([tp])[tp]
                    latest = consumer.end_offsets([tp])[tp]
                    
                    topic_stats['messages'] += latest - earliest
                    topic_stats['earliest_offset'] = min(topic_stats['earliest_offset'], earliest)
                    topic_stats['latest_offset'] = max(topic_stats['latest_offset'], latest)
                
                stats[topic] = topic_stats
            
            consumer.close()
            return stats
        except Exception as e:
            logging.error(f"获取topic统计信息失败: {e}")
            return {}

    def close(self):
        """关闭Kafka连接"""
        if hasattr(self, 'producer'):
            self.producer.close()

    def send_url_to_dispatcher(self, url, metadata=None, priority='medium'):
        """将URL发送到分发系统"""
        if not url:
            return False
        
        # 确定主题
        topic = f"douban_{self.url_type}_{priority}"
        
        # 准备数据
        data = {
            "url": url,
            "priority": priority,
            "topic": topic,
            "timestamp": int(time.time())
        }
        
        # 添加元数据
        if metadata and isinstance(metadata, dict):
            data.update(metadata)
        
        # 发送到Kafka
        try:
            self.producer.send(topic, data)
            self.producer.flush()
            logging.info(f"URL已发送到分发系统: {url}, 优先级: {priority}")
            return True
        except Exception as e:
            logging.error(f"发送URL到分发系统失败: {e}")
            return False 