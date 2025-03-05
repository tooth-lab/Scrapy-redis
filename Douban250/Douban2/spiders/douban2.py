import scrapy
from scrapy_redis.spiders import RedisSpider
from Douban2.items import Douban2Item
import logging
import json
import redis
from scrapy.exceptions import DontCloseSpider
from scrapy import signals
from scrapy.http import HtmlResponse

class Douban2Spider(RedisSpider):
    name = 'douban2'
    redis_key = 'douban2:start_urls'
    
    def __init__(self, node_id=None, redis_host='localhost', redis_port=6379, *args, **kwargs):
        super(Douban2Spider, self).__init__(*args, **kwargs)
        self.node_id = node_id
        self._logger = logging.getLogger(self.name)
        self._logger.setLevel(logging.DEBUG)
        self._logger.info(f"爬虫节点 {self.node_id} 初始化完成")
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(Douban2Spider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider
    
    def spider_opened(self, spider):
        self._logger.info("爬虫开始运行")
        
    def spider_closed(self, spider):
        self._logger.info("爬虫已关闭")
    
    def make_request_from_data(self, data):
        """处理从Redis获取的URL数据"""
        try:
            if isinstance(data, bytes):
                data = data.decode('utf-8')
            
            self._logger.debug(f"处理URL数据: {data}")
            
            # 尝试解析JSON数据
            try:
                url_data = json.loads(data)
                if isinstance(url_data, dict):
                    url = url_data.get('url', '')
                else:
                    url = data
            except json.JSONDecodeError:
                url = data
            
            if not url:
                self._logger.warning(f"无效的URL数据: {data}")
                return None
            
            self._logger.info(f"创建请求: {url}")
            return scrapy.Request(
                url=url,
                callback=self.parse,
                dont_filter=True,
                meta={
                    'dont_redirect': True,
                    'handle_httpstatus_list': [301, 302]
                },
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive'
                },
                errback=self.errback_httpbin
            )
            
        except Exception as e:
            self._logger.error(f"处理URL数据时出错: {str(e)}, 数据: {data}")
            return None

    def errback_httpbin(self, failure):
        """处理请求错误"""
        self._logger.error(f"请求失败: {str(failure.value)}")
        if failure.request:
            self._logger.error(f"失败的URL: {failure.request.url}")

    def parse(self, response):
        """解析电影列表页面"""
        self._logger.info(f"开始解析页面: {response.url}")
        try:
            # 确保响应是HTML格式
            if not isinstance(response, HtmlResponse):
                self._logger.error(f"响应不是HTML格式: {response.url}")
                return
                
            # 检查响应内容
            if not response.text:
                self._logger.error(f"响应内容为空: {response.url}")
                return
                
            # 使用xpath提取电影列表
            movie_list = response.xpath('//ol[@class="grid_view"]/li')
            self._logger.info(f"找到 {len(movie_list)} 个电影条目")
            
            for movie in movie_list:
                item = Douban2Item()
                
                # 提取基本信息
                item['rank'] = movie.xpath('.//em/text()').get()
                
                # 检查排名，只处理排名1-250的电影
                try:
                    rank = int(item['rank'])
                    if rank > 250:
                        self._logger.info(f"跳过排名 {rank} 的电影，超出了1-250的范围")
                        continue
                except (ValueError, TypeError):
                    self._logger.warning(f"无法解析电影排名: {item['rank']}")
                    continue
                
                item['name'] = movie.xpath('.//span[@class="title"][1]/text()').get()
                item['director'] = movie.xpath('.//div[@class="bd"]/p[1]/text()').get()
                item['rate'] = movie.xpath('.//span[@class="rating_num"]/text()').get()
                item['num'] = movie.xpath('.//span[contains(@class, "rating_num")]/following-sibling::span/text()').get()
                
                # 获取详情页URL
                detail_url = movie.xpath('.//div[@class="hd"]/a/@href').get()
                
                if detail_url:
                    # 设置优先级 - 对于1-250的电影，根据排名设置优先级
                    if rank <= 100:
                        priority = 1
                    elif rank <= 200:
                        priority = 0
                    else:
                        priority = -1
                    
                    yield scrapy.Request(
                        url=detail_url,
                        callback=self.parse_detail,
                        meta={
                            'item': item,
                            'dont_redirect': True,
                            'handle_httpstatus_list': [301, 302]
                        },
                        headers={
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                            'Accept-Encoding': 'gzip, deflate',
                            'Connection': 'keep-alive'
                        },
                        priority=priority,
                        dont_filter=True
                    )
                    
        except Exception as e:
            self._logger.error(f"解析页面出错: {str(e)}, URL: {response.url}")
            self._logger.exception(e)

    def parse_detail(self, response):
        """解析电影详情页面"""
        if not isinstance(response, HtmlResponse):
            self._logger.error(f"响应不是HTML格式: {response.url}")
            return
            
        item = response.meta['item']
        try:
            # 提取详细信息
            item['director'] = response.xpath('//span[text()="导演"]/following-sibling::span[@class="attrs"]/a/text()').get()
            item['screenwriter'] = response.xpath('//span[text()="编剧"]/following-sibling::span[@class="attrs"]/a/text()').getall()
            item['actors'] = response.xpath('//span[text()="主演"]/following-sibling::span[@class="attrs"]/a/text()').getall()
            item['genres'] = response.xpath('//span[text()="类型:"]/following-sibling::span[@property="v:genre"]/text()').getall()
            item['country'] = response.xpath('//span[text()="制片国家/地区:"]/following-sibling::text()').get()
            item['language'] = response.xpath('//span[text()="语言:"]/following-sibling::text()').get()
            item['release_date'] = response.xpath('//span[text()="上映日期:"]/following-sibling::span[@property="v:initialReleaseDate"]/text()').get()
            item['runtime'] = response.xpath('//span[text()="片长:"]/following-sibling::span[@property="v:runtime"]/text()').get()
            item['imdb'] = response.xpath('//span[text()="IMDb:"]/following-sibling::text()').get()
            
            # 清理数据
            for field in item.fields:
                if isinstance(item.get(field), str):
                    item[field] = item[field].strip() if item[field] else ''
            
            self._logger.info(f"成功解析电影: {item['name']} (排名: {item['rank']})")
            yield item
            
        except Exception as e:
            self._logger.error(f"解析详情页出错: {str(e)}, URL: {response.url}")
            self._logger.exception(e)