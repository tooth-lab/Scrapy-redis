# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter

import random
import redis
import logging
import socket
import time
from urllib.parse import urlparse
from scrapy.exceptions import IgnoreRequest
from twisted.internet.error import TimeoutError, DNSLookupError
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message


class Douban2SpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn't have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class Douban2DownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class RandomUserAgentMiddleware:
    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15'
        ]
        self.cookies = {
            'bid': self.generate_bid(),
            'll': '"108288"',
            'ap_v': '0,6.0',
            'ct': 'y',
            '__utma': '30149280.1.1.1616817008.1616817008.1616817008.1',
            '__utmb': '30149280.0.10.1616817008',
            '__utmc': '30149280',
            '__utmz': '30149280.1616817008.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)',
        }

    def generate_bid(self):
        """生成随机bid"""
        import random
        import string
        return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(11))
        
    def process_request(self, request, spider):
        ua = random.choice(self.user_agents)
        request.headers['User-Agent'] = ua
        request.headers['Host'] = 'movie.douban.com'
        request.headers['Connection'] = 'keep-alive'
        request.headers['sec-ch-ua'] = '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"'
        request.headers['sec-ch-ua-mobile'] = '?0'
        request.headers['sec-ch-ua-platform'] = '"Windows"'
        request.headers['Sec-Fetch-Dest'] = 'document'
        request.headers['Sec-Fetch-Mode'] = 'navigate'
        request.headers['Sec-Fetch-Site'] = 'none'
        request.headers['Sec-Fetch-User'] = '?1'
        
        # 更新Cookie
        self.cookies['bid'] = self.generate_bid()
        request.cookies = self.cookies


class IPRoyalProxyMiddleware:
    def __init__(self, settings):
        from utils.proxy_pool import get_proxy_pool
        self.proxy_pool = get_proxy_pool()
        self.logger = logging.getLogger(__name__)
        self.last_proxy_time = {}  # 记录每个代理的最后使用时间
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'retry_count': 0
        }

    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls(crawler.settings)
        # 注册统计信息收集器
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    def process_request(self, request, spider):
        """处理请求"""
        self.stats['total_requests'] += 1
        
        # 获取代理
        proxy = self.proxy_pool.get_proxy()
        if not proxy:
            self.logger.error("无法获取代理")
            return None
            
        # 记录代理使用时间
        current_time = time.time()
        self.last_proxy_time[proxy] = current_time
        
        # 设置代理
        request.meta['proxy'] = proxy
        request.meta['proxy_start_time'] = current_time
        request.meta['download_slot'] = proxy  # 确保每个代理的请求队列独立
        
        self.logger.debug(f"使用代理 {proxy} 访问: {request.url}")

    def process_response(self, request, response, spider):
        """处理响应"""
        proxy = request.meta.get('proxy')
        if not proxy:
            return response
            
        # 计算响应时间
        start_time = request.meta.get('proxy_start_time', 0)
        response_time = time.time() - start_time if start_time else None
        
        # 处理不同状态码
        if response.status in [200, 301, 302]:
            self.stats['successful_requests'] += 1
            self.proxy_pool.report_proxy_status(proxy, success=True, response_time=response_time)
            return response
            
        elif response.status in [403, 418]:
            self.logger.warning(f"代理 {proxy} 被目标站点限制")
            self.stats['failed_requests'] += 1
            self.proxy_pool.report_proxy_status(proxy, success=False)
            
            # 强制更换代理重试
            new_proxy = self.proxy_pool.get_proxy()
            if new_proxy:
                request.meta['proxy'] = new_proxy
                request.meta['proxy_start_time'] = time.time()
                request.dont_filter = True
                return request
                
        elif response.status >= 500:
            self.logger.error(f"服务器错误 {response.status}")
            self.stats['failed_requests'] += 1
            return response
            
        return response

    def process_exception(self, request, exception, spider):
        """处理异常"""
        proxy = request.meta.get('proxy')
        if not proxy:
            return None
            
        self.logger.error(f"代理 {proxy} 请求异常: {str(exception)}")
        self.stats['failed_requests'] += 1
        self.proxy_pool.report_proxy_status(proxy, success=False)
        
        # 重试逻辑
        if self.stats['retry_count'] < 3:  # 最多重试3次
            self.stats['retry_count'] += 1
            new_proxy = self.proxy_pool.get_proxy()
            if new_proxy:
                request.meta['proxy'] = new_proxy
                request.meta['proxy_start_time'] = time.time()
                request.dont_filter = True
                return request
        
        return None

    def spider_closed(self, spider):
        """爬虫关闭时的统计"""
        self.logger.info("\n代理使用统计:")
        self.logger.info(f"总请求数: {self.stats['total_requests']}")
        self.logger.info(f"成功请求: {self.stats['successful_requests']}")
        self.logger.info(f"失败请求: {self.stats['failed_requests']}")
        self.logger.info(f"重试次数: {self.stats['retry_count']}")
        
        if self.stats['total_requests'] > 0:
            success_rate = (self.stats['successful_requests'] / self.stats['total_requests']) * 100
            self.logger.info(f"成功率: {success_rate:.2f}%")
            
        # 获取代理池统计
        proxy_stats = self.proxy_pool.get_proxy_stats()
        if proxy_stats:
            self.logger.info("\n代理池统计:")
            self.logger.info(f"可用代理数: {proxy_stats['available']}")
            self.logger.info(f"失败代理数: {proxy_stats['failed']}")
            self.logger.info(f"总请求数: {proxy_stats['total_requests']}")


class DistributedStatsMiddleware:
    def process_request(self, request, spider):
        # 记录各个节点的状态
        spider.server.hincrby(f"{spider.name}:stats", 
                            f"node_{socket.gethostname()}", 1)


class DNSCacheMiddleware:
    def __init__(self):
        from utils.dns_cache import DNSCache
        self.dns_cache = DNSCache()
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_request(self, request, spider):
        url = request.url
        ip = self.dns_cache.resolve_url(url)
        if ip:
            self.logger.debug(f"DNS缓存命中: {url} -> {ip}")
            request.meta['dns_cache'] = ip
            # 可以选择直接修改请求的URL为IP
            # request.url = request.url.replace(urlparse(url).netloc, ip)


class RobotsMiddleware:
    def __init__(self):
        from utils.robots_parser import RobotsParser
        self.robots_parser = RobotsParser()
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_request(self, request, spider):
        url = request.url
        if not self.robots_parser.can_fetch(url):
            self.logger.warning(f"Robots协议禁止访问: {url}")
            raise IgnoreRequest(f"Robots协议禁止访问: {url}")
        
        delay = self.robots_parser.get_crawl_delay(url)
        if delay:
            spider.logger.debug(f"Robots延迟: {delay}秒")
            time.sleep(delay)


class EnhancedIPRoyalProxyMiddleware:
    def __init__(self, settings):
        self.proxy_config = settings.get('IPROYAL_PROXY', {})
        self.min_delay = settings.getfloat('DOWNLOAD_DELAY', 5)
        self.redis_client = redis.Redis(
            host=settings.get('REDIS_HOST', 'localhost'),
            port=settings.get('REDIS_PORT', 6379),
            db=0
        )
        self.last_request_time = {}
        self.request_count = {}
        self.max_requests_per_proxy = 50  # 每个代理最大请求数
        
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)
    
    def _get_proxy_url(self):
        """构建代理URL"""
        config = self.proxy_config
        proxy_url = f"http://{config['username']}:{config['password']}@{config['host']}:{config['port']}"
        return proxy_url
    
    def _should_rotate_proxy(self, proxy):
        """判断是否需要轮换代理"""
        count = self.request_count.get(proxy, 0)
        return count >= self.max_requests_per_proxy
    
    def _enforce_delay(self, proxy):
        """强制延迟检查"""
        current_time = time.time()
        last_time = self.last_request_time.get(proxy, 0)
        
        # 添加随机延迟
        delay = self.min_delay + random.uniform(1, 3)
        if current_time - last_time < delay:
            time.sleep(delay - (current_time - last_time))
        
        self.last_request_time[proxy] = time.time()
    
    def process_request(self, request, spider):
        # 检查请求频率
        if not self._is_request_allowed(request):
            raise IgnoreRequest("请求频率过高")
        
        # 获取代理
        proxy = self._get_proxy_url()
        
        # 检查是否需要轮换代理
        if self._should_rotate_proxy(proxy):
            # 重置计数
            self.request_count[proxy] = 0
            time.sleep(random.uniform(2, 5))  # 轮换延迟
        
        # 强制延迟
        self._enforce_delay(proxy)
        
        # 设置代理
        request.meta['proxy'] = proxy
        
        # 更新请求计数
        self.request_count[proxy] = self.request_count.get(proxy, 0) + 1
        
        # 添加随机User-Agent
        request.headers['User-Agent'] = self._get_random_ua()
        
        # 添加其他headers
        self._add_random_headers(request)
    
    def _is_request_allowed(self, request):
        """检查请求频率限制"""
        domain = request.url.split('/')[2]
        key = f'request_frequency:{domain}'
        
        # 使用Redis进行频率限制
        current_count = self.redis_client.incr(key)
        if current_count == 1:
            self.redis_client.expire(key, 60)  # 60秒过期
        
        return current_count <= 30  # 每分钟最多30个请求
    
    def _get_random_ua(self):
        """获取随机User-Agent"""
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            # 添加更多真实的UA
        ]
        return random.choice(user_agents)
    
    def _add_random_headers(self, request):
        """添加随机headers"""
        request.headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        request.headers['Accept-Language'] = 'zh-CN,zh;q=0.9,en;q=0.8'
        request.headers['Cache-Control'] = 'max-age=0'
        request.headers['Connection'] = 'keep-alive'
        
        # 随机添加一些额外headers
        if random.random() < 0.3:
            request.headers['Accept-Encoding'] = 'gzip, deflate, br'
        if random.random() < 0.3:
            request.headers['Upgrade-Insecure-Requests'] = '1'
    
    def process_response(self, request, response, spider):
        proxy = request.meta.get('proxy', '')
        
        # 检查响应状态
        if response.status in [403, 429, 503]:
            logger.warning(f"代理 {proxy} 被封禁或请求过多")
            # 标记代理需要轮换
            self.request_count[proxy] = self.max_requests_per_proxy
            # 重试请求
            request.dont_filter = True
            return request
            
        return response
    
    def process_exception(self, request, exception, spider):
        if isinstance(exception, (TimeoutError, DNSLookupError)):
            # 代理超时，标记需要轮换
            proxy = request.meta.get('proxy', '')
            self.request_count[proxy] = self.max_requests_per_proxy
            
            # 重试请求
            request.dont_filter = True
            return request
            
class EnhancedRetryMiddleware(RetryMiddleware):
    def __init__(self, settings):
        super().__init__(settings)
        self.redis_client = redis.Redis(
            host=settings.get('REDIS_HOST', 'localhost'),
            port=settings.get('REDIS_PORT', 6379),
            db=0
        )
    
    def _retry(self, request, reason, spider):
        retries = request.meta.get('retry_times', 0)
        
        # 记录重试信息
        retry_key = f'retry:{request.url}'
        self.redis_client.hincrby('retry_stats', retry_key, 1)
        
        # 增加随机延迟
        time.sleep(random.uniform(2, 5) * (retries + 1))
        
        return super()._retry(request, reason, spider)
