import urllib.robotparser
import redis
import json
import time
import logging
import requests
from urllib.parse import urlparse
from threading import Thread

class RobotsParser:
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
        self.logger = logging.getLogger('robots_parser')
        self.logger.setLevel(logging.INFO)
        
        # Redis配置
        self.redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        self.cache_key_prefix = 'robots_cache:'
        
        # 缓存时间（24小时）
        self.cache_ttl = 24 * 3600
        
        # 启动清理线程
        self.cleanup_thread = Thread(target=self._cleanup_expired_entries)
        self.cleanup_thread.daemon = True
        self.cleanup_thread.start()
        
    def get_robots_url(self, url):
        """获取网站的robots.txt URL"""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        
    def fetch_robots_txt(self, url):
        """获取robots.txt内容"""
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.text
            return None
        except Exception as e:
            self.logger.error(f"获取robots.txt失败: {url} - {str(e)}")
            return None
            
    def can_fetch(self, url, user_agent='*'):
        """检查URL是否允许爬取"""
        domain = urlparse(url).netloc
        cache_key = f"{self.cache_key_prefix}{domain}"
        
        # 检查缓存
        cached = self.redis.get(cache_key)
        if cached:
            data = json.loads(cached)
            if time.time() < data['expire_time']:
                rp = urllib.robotparser.RobotFileParser()
                rp.parse(data['rules'])
                return rp.can_fetch(user_agent, url)
        
        # 获取并解析robots.txt
        robots_url = self.get_robots_url(url)
        robots_content = self.fetch_robots_txt(robots_url)
        
        if robots_content:
            rp = urllib.robotparser.RobotFileParser()
            rp.parse(robots_content.splitlines())
            
            # 更新缓存
            cache_data = {
                'rules': robots_content.splitlines(),
                'expire_time': time.time() + self.cache_ttl
            }
            self.redis.set(cache_key, json.dumps(cache_data))
            
            return rp.can_fetch(user_agent, url)
        
        # 如果无法获取robots.txt，默认允许访问
        return True
        
    def get_crawl_delay(self, url, user_agent='*'):
        """获取网站的爬取延迟要求"""
        domain = urlparse(url).netloc
        cache_key = f"{self.cache_key_prefix}{domain}"
        
        # 检查缓存
        cached = self.redis.get(cache_key)
        if cached:
            data = json.loads(cached)
            if time.time() < data['expire_time']:
                rp = urllib.robotparser.RobotFileParser()
                rp.parse(data['rules'])
                return rp.crawl_delay(user_agent)
        
        # 获取并解析robots.txt
        robots_url = self.get_robots_url(url)
        robots_content = self.fetch_robots_txt(robots_url)
        
        if robots_content:
            rp = urllib.robotparser.RobotFileParser()
            rp.parse(robots_content.splitlines())
            
            # 更新缓存
            cache_data = {
                'rules': robots_content.splitlines(),
                'expire_time': time.time() + self.cache_ttl
            }
            self.redis.set(cache_key, json.dumps(cache_data))
            
            return rp.crawl_delay(user_agent)
        
        # 如果无法获取robots.txt，返回默认延迟（1秒）
        return 1
        
    def _cleanup_expired_entries(self):
        """清理过期的缓存"""
        while True:
            try:
                pattern = f"{self.cache_key_prefix}*"
                keys = self.redis.keys(pattern)
                
                now = time.time()
                for key in keys:
                    try:
                        data = json.loads(self.redis.get(key))
                        if now >= data['expire_time']:
                            self.redis.delete(key)
                            self.logger.debug(f"清理过期robots缓存: {key}")
                    except:
                        continue
                        
            except Exception as e:
                self.logger.error(f"清理robots缓存出错: {str(e)}")
                
            # 每小时清理一次
            time.sleep(3600)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 测试代码
    parser = RobotsParser()
    test_url = "https://movie.douban.com/top250"
    
    print(f"是否允许爬取: {parser.can_fetch(test_url)}")
    print(f"爬取延迟: {parser.get_crawl_delay(test_url)} 秒") 