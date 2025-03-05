# Scrapy settings for Douban2 project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'Douban2'

SPIDER_MODULES = ['Douban2.spiders']
NEWSPIDER_MODULE = 'Douban2.spiders'

# 基本配置
ROBOTSTXT_OBEY = False  # 修改为False，因为我们已经在中间件中处理了robots
DOWNLOAD_DELAY = 2  # 每个节点的下载延迟
RANDOMIZE_DOWNLOAD_DELAY = True
CONCURRENT_REQUESTS = 2  # 每个节点的并发请求数
CONCURRENT_REQUESTS_PER_DOMAIN = 2
CONCURRENT_REQUESTS_PER_IP = 2

# 启用Item Pipeline
ITEM_PIPELINES = {
    'Douban2.pipelines.Douban2Pipeline': 300,
}

# 启用自动限速
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 3
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0
AUTOTHROTTLE_DEBUG = True

# 重试设置
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 429]

# 启用Cookie
COOKIES_ENABLED = True  # 启用Cookie
COOKIES_DEBUG = True

# 下载器中间件
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': 700,  # 启用Cookie中间件
    'Douban2.middlewares.RandomUserAgentMiddleware': 400,
    'Douban2.middlewares.IPRoyalProxyMiddleware': 350,  # 添加代理中间件
    'Douban2.middlewares.DNSCacheMiddleware': 550,
    'Douban2.middlewares.RobotsMiddleware': 100,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
    'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
    'scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware': 400,
    'Douban2.middlewares.DistributedStatsMiddleware': 850,
}

# Redis配置
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_ENCODING = 'utf-8'
REDIS_PARAMS = {
    'socket_timeout': 30,
    'socket_connect_timeout': 30,
    'retry_on_timeout': True,
    'encoding': 'utf-8'
}

# Scrapy-Redis配置
SCHEDULER = "scrapy_redis.scheduler.Scheduler"
DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"
SCHEDULER_PERSIST = True
SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.SpiderPriorityQueue'

# Redis去重配置
DUPEFILTER_KEY = '%(spider)s:dupefilter'
SCHEDULER_FLUSH_ON_START = False
SCHEDULER_IDLE_BEFORE_CLOSE = 10  # 改为10秒

# 启用更多调试信息
DUPEFILTER_DEBUG = True
SCHEDULER_DEBUG = True

# 队列配置
SCHEDULER_QUEUE_KEY = '%(spider)s:requests'
SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.SpiderPriorityQueue'
SCHEDULER_SERIALIZER = "scrapy_redis.picklecompat"

# Redis连接URL
REDIS_URL = 'redis://localhost:6379'

# Redis参数
REDIS_START_URLS_AS_SET = False
REDIS_START_URLS_KEY = '%(name)s:start_urls'

# 每个节点的日志设置
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'

# 不要设置固定的LOG_FILE，让每个节点使用自己的日志文件
# LOG_FILE = 'logs/spider_node1.log'  # 注释掉这行

# MySQL配置
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'douban_spider'
}

# 爬取250个项目后关闭
CLOSESPIDER_ITEMCOUNT = 250  # 收集250个项目后关闭
CLOSESPIDER_TIMEOUT = 1800  # 30分钟后强制关闭
CLOSESPIDER_ERRORCOUNT = 20  # 出现20个错误后关闭

# 启用重定向
REDIRECT_ENABLED = True  # 启用重定向
REDIRECT_MAX_TIMES = 3  # 最大重定向次数

# 增加下载超时时间
DOWNLOAD_TIMEOUT = 15  # 增加超时时间

# 更多的请求头
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'Referer': 'https://movie.douban.com/'
}

# Kafka设置
KAFKA_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'douban_spider'

STATS_DUMP = True
STATS_DUMP_INTERVAL = 60.0

# 代理设置
IPROYAL_PROXY = {
    'username': 'wvR6upmH4hiV9OSo',  # 替换为你的IPRoyal用户名
    'password': '80kOlNDhuUFnLpJ3_country-cn',  # 替换为你的IPRoyal密码
    'host': 'geo.iproyal.com',    # 替换为IPRoyal主机地址
    'port': '12321',    # 替换为IPRoyal端口
    'country': 'cn',              # 指定中国IP
    'protocol': 'https',          # 使用HTTPS协议
    'session': 'rotating',        # 使用轮转会话
    'rotation_interval': 60,      # 1分钟轮转一次
    'min_ip_reuse_interval': 300  # 同一IP最少5分钟后才能重用
}
