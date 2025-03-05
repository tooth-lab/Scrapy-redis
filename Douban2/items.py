# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class Douban2Item(Item):
    """
    豆瓣电影数据模型
    定义了爬取的电影信息的数据结构
    """
    name = Field()         # 电影名称
    director = Field()     # 导演
    screenwriter = Field() # 编剧
    actors = Field()       # 主演
    genres = Field()       # 类型
    country = Field()      # 制片国家/地区
    language = Field()     # 语言
    release_date = Field() # 上映日期
    runtime = Field()      # 片长
    imdb = Field()        # IMDb链接
    rate = Field()        # 评分
    num = Field()         # 评价人数
    rank = Field()        # 排名
    crawl_time = Field()  # 爬取时间
    node_id = Field()     # 爬虫节点ID