import json
import logging
import time
import threading
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CrawlerNodeManager:
    def __init__(self, bootstrap_servers='localhost:9092', node_id=None):
        self.bootstrap_servers = bootstrap_servers
        self.node_id = node_id or f"node-{int(time.time())}"
        self.consumer = KafkaConsumer(
            'url_dispatched',
            bootstrap_servers=bootstrap_servers,
            group_id=f'crawler-{self.node_id}',
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # 节点状态
        self.status = {
            "node_id": self.node_id,
            "load": 0,
            "capacity": 100,
            "urls_processed": 0,
            "last_heartbeat": int(time.time())
        }
        
        # 启动心跳线程
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
    
    def send_heartbeat(self):
        """定期发送心跳信息"""
        while True:
            try:
                self.status["last_heartbeat"] = int(time.time())
                self.producer.send('crawler_heartbeat', self.status)
                logger.debug(f"发送心跳: {self.status}")
                time.sleep(10)  # 每10秒发送一次心跳
            except Exception as e:
                logger.error(f"发送心跳失败: {e}")
                time.sleep(5)
    
    def process_url(self, url_data):
        """处理分发的URL"""
        try:
            # 检查URL是否分配给当前节点
            if url_data.get("node_id") != self.node_id:
                return
            
            url = url_data.get("url", "")
            priority = url_data.get("priority", "medium")
            
            # 模拟处理URL
            logger.info(f"处理URL: {url}, 优先级: {priority}")
            
            # 更新节点状态
            self.status["load"] += 1
            self.status["urls_processed"] += 1
            
            # 模拟处理完成
            time.sleep(2)  # 模拟处理时间
            
            # 处理完成后减少负载
            self.status["load"] -= 1
            
            # 发送处理结果
            result = {
                "url": url,
                "node_id": self.node_id,
                "status": "completed",
                "timestamp": int(time.time())
            }
            self.producer.send('url_processed', result)
            
        except Exception as e:
            logger.error(f"处理URL失败: {e}")
    
    def start(self):
        """启动节点管理器"""
        logger.info(f"启动爬虫节点 {self.node_id}")
        
        try:
            for message in self.consumer:
                url_data = message.value
                # 创建新线程处理URL
                threading.Thread(target=self.process_url, args=(url_data,)).start()
        except Exception as e:
            logger.error(f"节点管理器运行失败: {e}")
        finally:
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    import sys
    node_id = sys.argv[1] if len(sys.argv) > 1 else None
    node_manager = CrawlerNodeManager(node_id=node_id)
    node_manager.start() 