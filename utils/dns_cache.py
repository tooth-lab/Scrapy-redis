import dns.resolver
import redis
import json
import time
import logging
from threading import Thread
import socket
from urllib.parse import urlparse

class DNSCache:
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
        self.logger = logging.getLogger('dns_cache')
        self.logger.setLevel(logging.INFO)
        
        # Redis配置
        self.redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        self.cache_key_prefix = 'dns_cache:'
        
        # DNS解析器配置
        self.resolver = dns.resolver.Resolver()
        self.resolver.timeout = 3
        self.resolver.lifetime = 3
        
        # 默认TTL (1小时)
        self.default_ttl = 3600
        
        # 启动清理线程
        self.cleanup_thread = Thread(target=self._cleanup_expired_entries)
        self.cleanup_thread.daemon = True
        self.cleanup_thread.start()
        
    def get_domain(self, url):
        """从URL中提取域名"""
        return urlparse(url).netloc
        
    def get_ip(self, domain):
        """获取域名对应的IP地址"""
        cache_key = f"{self.cache_key_prefix}{domain}"
        
        # 尝试从缓存获取
        cached = self.redis.get(cache_key)
        if cached:
            data = json.loads(cached)
            if time.time() < data['expire_time']:
                self.logger.debug(f"DNS缓存命中: {domain} -> {data['ip']}")
                return data['ip']
        
        # 缓存未命中，进行DNS解析
        try:
            answers = self.resolver.resolve(domain, 'A')
            ip = str(answers[0])
            
            # 获取TTL
            ttl = min(answers.rrset.ttl, self.default_ttl)
            
            # 更新缓存
            cache_data = {
                'ip': ip,
                'expire_time': time.time() + ttl
            }
            self.redis.set(cache_key, json.dumps(cache_data))
            
            self.logger.info(f"DNS解析成功: {domain} -> {ip} (TTL: {ttl}s)")
            return ip
            
        except dns.resolver.NXDOMAIN:
            self.logger.error(f"域名不存在: {domain}")
            return None
        except dns.resolver.Timeout:
            self.logger.error(f"DNS解析超时: {domain}")
            return None
        except Exception as e:
            self.logger.error(f"DNS解析错误: {domain} - {str(e)}")
            return None
            
    def _cleanup_expired_entries(self):
        """清理过期的DNS缓存"""
        while True:
            try:
                # 获取所有缓存键
                pattern = f"{self.cache_key_prefix}*"
                keys = self.redis.keys(pattern)
                
                now = time.time()
                for key in keys:
                    try:
                        data = json.loads(self.redis.get(key))
                        if now >= data['expire_time']:
                            self.redis.delete(key)
                            self.logger.debug(f"清理过期DNS缓存: {key}")
                    except:
                        continue
                        
            except Exception as e:
                self.logger.error(f"清理DNS缓存出错: {str(e)}")
                
            # 每10分钟清理一次
            time.sleep(600)
            
    def resolve_url(self, url):
        """解析URL获取IP地址"""
        domain = self.get_domain(url)
        if not domain:
            return None
        return self.get_ip(domain)
        
    def prefetch_dns(self, domains):
        """预获取多个域名的DNS记录"""
        for domain in domains:
            try:
                self.get_ip(domain)
            except:
                continue

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 测试代码
    dns_cache = DNSCache()
    test_urls = [
        "https://www.douban.com",
        "https://movie.douban.com",
        "https://book.douban.com"
    ]
    
    for url in test_urls:
        ip = dns_cache.resolve_url(url)
        print(f"{url} -> {ip}") 