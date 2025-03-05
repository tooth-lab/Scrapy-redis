import redis
import logging
import time
import requests
import random
from urllib.parse import urlparse

class ProxyPool:
    def __init__(self, 
                 host='localhost',
                 port=6379,
                 db=0,
                 proxy_username='wvR6upmH4hiV9OSo',  # 替换为你的IPRoyal用户名
                 proxy_password='80kOlNDhuUFnLpJ3_country-cn',  # 替换为你的IPRoyal密码
                 proxy_host='geo.iproyal.com',   # 替换为IPRoyal主机地址
                 proxy_port='11201'):  # 替换为IPRoyal端口
                 
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True
        )
        self.logger = logging.getLogger(__name__)
        
        # IPRoyal代理配置
        self.proxy_auth = {
            'username': proxy_username,
            'password': proxy_password,
            'host': proxy_host,
            'port': proxy_port
        }
        
        # Redis键
        self.proxy_key = 'iproyal:proxies:available'
        self.proxy_failed_key = 'iproyal:proxies:failed'
        self.proxy_stats_key = 'iproyal:proxies:stats'
        
        # 初始化代理
        self._init_proxies()
        
    def _init_proxies(self):
        """初始化代理池"""
        proxy_url = f"http://{self.proxy_auth['username']}:{self.proxy_auth['password']}@{self.proxy_auth['host']}:{self.proxy_auth['port']}"
        self.redis_client.sadd(self.proxy_key, proxy_url)
        self.logger.info("代理池初始化完成")
        
    def get_proxy(self):
        """获取一个可用代理"""
        try:
            proxy = self.redis_client.srandmember(self.proxy_key)
            if not proxy:
                self._init_proxies()
                proxy = self.redis_client.srandmember(self.proxy_key)
            
            # 更新使用统计
            self.redis_client.hincrby(self.proxy_stats_key, proxy, 1)
            
            return proxy
        except Exception as e:
            self.logger.error(f"获取代理失败: {str(e)}")
            return None
            
    def report_proxy_status(self, proxy, success=True, response_time=None):
        """报告代理使用状态"""
        try:
            if success:
                # 记录成功次数和响应时间
                self.redis_client.hincrby(f"{self.proxy_stats_key}:success", proxy, 1)
                if response_time:
                    self.redis_client.hset(f"{self.proxy_stats_key}:response_time", 
                                         proxy, 
                                         response_time)
            else:
                # 记录失败次数
                self.redis_client.hincrby(f"{self.proxy_stats_key}:failed", proxy, 1)
                failed_count = int(self.redis_client.hget(
                    f"{self.proxy_stats_key}:failed", proxy) or 0)
                
                # 如果失败次数过多，暂时禁用该代理
                if failed_count > 5:
                    self.redis_client.smove(self.proxy_key, 
                                          self.proxy_failed_key,
                                          proxy)
                    self.logger.warning(f"代理 {proxy} 失败次数过多，已禁用")
        except Exception as e:
            self.logger.error(f"更新代理状态失败: {str(e)}")
            
    def test_proxy(self, proxy):
        """测试代理是否可用"""
        try:
            start_time = time.time()
            response = requests.get(
                'https://movie.douban.com/robots.txt',
                proxies={'https': proxy},
                timeout=10
            )
            response_time = time.time() - start_time
            
            if response.status_code in [200, 403, 418]:  # 这些状态码表示代理正常工作
                self.logger.debug(f"代理 {proxy} 测试成功，响应时间: {response_time:.2f}秒")
                return True, response_time
            return False, response_time
        except Exception as e:
            self.logger.error(f"代理测试失败: {str(e)}")
            return False, None
            
    def get_proxy_stats(self):
        """获取代理使用统计"""
        try:
            stats = {
                'available': self.redis_client.scard(self.proxy_key),
                'failed': self.redis_client.scard(self.proxy_failed_key),
                'total_requests': sum(int(x) for x in 
                    self.redis_client.hvals(self.proxy_stats_key) or [0]),
                'success_rate': {}
            }
            
            # 计算每个代理的成功率
            for proxy in self.redis_client.smembers(self.proxy_key):
                success = int(self.redis_client.hget(
                    f"{self.proxy_stats_key}:success", proxy) or 0)
                failed = int(self.redis_client.hget(
                    f"{self.proxy_stats_key}:failed", proxy) or 0)
                total = success + failed
                if total > 0:
                    stats['success_rate'][proxy] = (success / total) * 100
                    
            return stats
        except Exception as e:
            self.logger.error(f"获取代理统计信息失败: {str(e)}")
            return None

# 创建代理池实例
proxy_pool = None

def get_proxy_pool():
    """获取代理池单例"""
    global proxy_pool
    if proxy_pool is None:
        proxy_pool = ProxyPool()
    return proxy_pool 