import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from url_manager.kafka_url_manager import KafkaURLManager
import logging
import redis
import json
from kafka import KafkaConsumer
from threading import Thread
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(level=logging.INFO)
    kafka_manager = KafkaURLManager()

    # 列表页URL
    list_urls = [
        ("https://movie.douban.com/top250?start=0", 1),  # 1-25名
        ("https://movie.douban.com/top250?start=25", 26),  # 26-50名
        ("https://movie.douban.com/top250?start=50", 51),  # 51-75名
        ("https://movie.douban.com/top250?start=75", 76),  # 76-100名
        ("https://movie.douban.com/top250?start=100", 101),  # 101-125名
        ("https://movie.douban.com/top250?start=125", 126),  # 126-150名
        ("https://movie.douban.com/top250?start=150", 151),  # 151-175名
        ("https://movie.douban.com/top250?start=175", 176),  # 176-200名
        ("https://movie.douban.com/top250?start=200", 201),  # 201-225名
        ("https://movie.douban.com/top250?start=225", 226)  # 226-250名
    ]

    for url, start_rank in list_urls:
        # 根据排名设置优先级
        if start_rank <= 100:
            priority = 'high'
        elif start_rank <= 200:
            priority = 'medium'
        else:
            priority = 'low'

        # 构建统一的消息格式
        message = {
            'url': url,
            'priority': priority,
            'timestamp': datetime.now().isoformat(),
            'metadata': {
                'page_type': 'list',
                'rank_start': start_rank,
                'priority': priority,
                'rank_end': start_rank + 24  # 每页25部电影
            }
        }

        kafka_manager.send_url(
            url=url,
            rank=start_rank,
            metadata=message['metadata'],
            priority=priority
        )
        logging.info(f"添加列表页URL到Kafka {priority}优先级: {url} (排名: {start_rank})")

    # 关闭连接
    kafka_manager.close()
    logging.info("所有URL已添加到Kafka")


def add_urls_to_redis():
    # Redis连接
    r = redis.Redis(host='localhost', port=6379, db=0)

    # 清空所有相关的Redis键
    r.delete('douban2:start_urls')
    r.delete('douban2:high_priority_urls')
    r.delete('douban2:low_priority_urls')
    r.delete('douban2:seen_urls')

    # 添加所有10页的URL（排名1-250）
    urls = [
        {
            'url': 'https://movie.douban.com/top250',  # 第一页
            'meta': {
                'page': 1,
                'priority': 'high',
                'start_rank': 1,
                'rank_start': 1,
                'rank_end': 25
            }
        },
        {
            'url': 'https://movie.douban.com/top250?start=25&filter=',  # 第二页
            'meta': {
                'page': 2,
                'priority': 'high',
                'start_rank': 26,
                'rank_start': 26,
                'rank_end': 50
            }
        },
        {
            'url': 'https://movie.douban.com/top250?start=50&filter=',  # 第三页
            'meta': {
                'page': 3,
                'priority': 'high',
                'start_rank': 51,
                'rank_start': 51,
                'rank_end': 75
            }
        },
        {
            'url': 'https://movie.douban.com/top250?start=75&filter=',  # 第四页
            'meta': {
                'page': 4,
                'priority': 'high',
                'start_rank': 76,
                'rank_start': 76,
                'rank_end': 100
            }
        },
        {
            'url': 'https://movie.douban.com/top250?start=100&filter=',  # 第五页
            'meta': {
                'page': 5,
                'priority': 'medium',
                'start_rank': 101,
                'rank_start': 101,
                'rank_end': 125
            }
        },
        {
            'url': 'https://movie.douban.com/top250?start=125&filter=',  # 第六页
            'meta': {
                'page': 6,
                'priority': 'medium',
                'start_rank': 126,
                'rank_start': 126,
                'rank_end': 150
            }
        },
        {
            'url': 'https://movie.douban.com/top250?start=150&filter=',  # 第七页
            'meta': {
                'page': 7,
                'priority': 'medium',
                'start_rank': 151,
                'rank_start': 151,
                'rank_end': 175
            }
        },
        {
            'url': 'https://movie.douban.com/top250?start=175&filter=',  # 第八页
            'meta': {
                'page': 8,
                'priority': 'medium',
                'start_rank': 176,
                'rank_start': 176,
                'rank_end': 200
            }
        },
        {
            'url': 'https://movie.douban.com/top250?start=200&filter=',  # 第九页
            'meta': {
                'page': 9,
                'priority': 'low',
                'start_rank': 201,
                'rank_start': 201,
                'rank_end': 225
            }
        },
        {
            'url': 'https://movie.douban.com/top250?start=225&filter=',  # 第十页
            'meta': {
                'page': 10,
                'priority': 'low',
                'start_rank': 226,
                'rank_start': 226,
                'rank_end': 250
            }
        }
    ]

    # 添加URL到Redis
    for url_data in urls:
        # 构建统一的消息格式
        message = {
            'url': url_data['url'],
            'priority': url_data['meta']['priority'],
            'timestamp': datetime.now().isoformat(),
            'metadata': url_data['meta']
        }

        # 将消息转换为JSON字符串
        message_json = json.dumps(message)

        # 根据优先级添加到相应的Redis队列
        priority = url_data['meta']['priority']
        if priority == 'high':
            r.lpush('douban2:high_priority_urls', message_json)
        elif priority == 'low':
            r.lpush('douban2:low_priority_urls', message_json)
        else:
            r.lpush('douban2:start_urls', message_json)

        # 添加到去重集合
        r.sadd('douban2:seen_urls', url_data['url'])

        print(
            f"添加URL到Redis: {url_data['url']}, 优先级: {priority}, 排名范围: {url_data['meta']['rank_start']}-{url_data['meta']['rank_end']}")

    print(f"成功添加 {len(urls)} 个URL到Redis的优先级队列")


def sync_kafka_to_redis():
    """
    同步Kafka中的URL到Redis，实现优先级队列到实时队列的转换
    """
    logger.info("启动Kafka到Redis的同步服务...")

    # Redis连接
    r = redis.Redis(host='localhost', port=6379, db=0)

    # 创建Kafka消费者，监听所有优先级的topic
    consumer = KafkaConsumer(
        'spider_urls_high',
        'spider_urls_medium',
        'spider_urls_low',
        bootstrap_servers='localhost:9092',
        group_id='url_sync_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    def process_message(message):
        try:
            # 获取URL数据
            url_data = message.value

            # 检查URL是否已存在
            url = url_data['url']
            if not r.sismember('douban2:seen_urls', url):
                # 添加到已见集合
                r.sadd('douban2:seen_urls', url)

                # 根据优先级添加到相应的Redis列表
                priority = url_data.get('priority', 'medium')
                if priority == 'high':
                    r.lpush('douban2:high_priority_urls', json.dumps(url_data))
                elif priority == 'low':
                    r.lpush('douban2:low_priority_urls', json.dumps(url_data))
                else:
                    r.lpush('douban2:start_urls', json.dumps(url_data))

                logger.info(f"URL同步到Redis成功: {url} (优先级: {priority})")
        except Exception as e:
            logger.error(f"处理消息失败: {e}")

    def priority_scheduler():
        """
        优先级调度器，定期将高优先级URL移动到活跃队列
        """
        while True:
            try:
                # 优先处理高优先级URL
                high_priority = r.rpop('douban2:high_priority_urls')
                if high_priority:
                    r.lpush('douban2:start_urls', high_priority)
                    continue

                # 其次处理普通URL（已在start_urls中）

                # 最后处理低优先级URL
                if not r.llen('douban2:start_urls'):
                    low_priority = r.rpop('douban2:low_priority_urls')
                    if low_priority:
                        r.lpush('douban2:start_urls', low_priority)

                time.sleep(1)  # 避免过于频繁的检查
            except Exception as e:
                logger.error(f"优先级调度失败: {e}")
                time.sleep(5)  # 出错后等待更长时间

    # 启动优先级调度器
    scheduler = Thread(target=priority_scheduler)
    scheduler.daemon = True
    scheduler.start()

    # 处理Kafka消息
    for message in consumer:
        process_message(message)


if __name__ == "__main__":
    try:
        # 启动Kafka到Redis的同步服务
        sync_thread = Thread(target=sync_kafka_to_redis)
        sync_thread.daemon = True
        sync_thread.start()

        # 添加URL到Kafka和Redis
        main()  # Kafka
        add_urls_to_redis()  # Redis
        print("URL初始化完成")
    except Exception as e:
        print(f"URL初始化失败: {e}")
    except KeyboardInterrupt:
        print("程序被用户中断")