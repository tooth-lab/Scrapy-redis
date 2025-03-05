# 豆瓣电影Top250分布式爬虫系统

基于Scrapy-Redis的分布式爬虫系统，用于爬取豆瓣电影Top250的详细信息。系统采用分布式架构，支持多节点协同工作，实现了高效、可靠的数据采集。

## 系统架构

系统分为六大核心模块：

### 1. URL管理与分发层
- URL智能分发器
- Kafka URL管理器
- Redis队列系统
- 支持URL优先级管理和去重机制

### 2. 数据采集层
- 主爬虫逻辑
- 中间件集合
- 代理池管理
- DNS缓存系统
- 反爬虫策略

### 3. 数据处理层
- 数据清洗
- 数据验证
- 数据转换
- 支持多种数据格式

### 4. 数据存储层
- MySQL主数据存储
- Parquet文件备份
- 自动备份机制
- 数据压缩和优化

### 5. 数据查询层
- 灵活的查询接口
- 多维度统计分析
- 支持多种导出格式

### 6. 监控管理层
- 系统资源监控
- 爬虫状态监控
- 异常报警机制
- 分布式节点管理

## 环境要求

- Python 3.8+
- Redis 6.0+
- MySQL 8.0+
- Kafka 2.8+
- Zookeeper 3.6+

## 依赖安装

```bash
pip install -r requirements.txt
```

## 配置说明

1. 数据库配置（settings.py）:
```python
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'douban_spider'
}
```

2. Redis配置:
```python
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
```

3. Kafka配置:
```python
KAFKA_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'douban_spider'
```

## 快速启动

1. 启动必要的服务:
```bash
# 启动Redis
redis-server

# 启动Zookeeper
zkServer.sh start

# 启动Kafka
kafka-server-start.sh config/server.properties
```

2. 启动爬虫系统:
```bash
python scripts/start_all.py
```

## 系统功能

1. URL管理
   - 支持优先级队列
   - 自动URL去重
   - 分布式协同工作

2. 数据采集
   - 自动代理切换
   - 请求频率控制
   - 异常重试机制

3. 数据处理
   - 自动数据清洗
   - 字段规范化
   - 数据验证

4. 数据存储
   - 多格式存储
   - 自动备份
   - 数据压缩

5. 数据查询
   - 灵活查询接口
   - 统计分析功能
   - 数据导出功能

6. 系统监控
   - 实时状态监控
   - 异常报警
   - 性能分析

## 项目结构

```
Douban2/
├── spiders/
│   └── douban2.py         # 主爬虫逻辑
├── items.py               # 数据模型定义
├── middlewares.py         # 中间件集合
├── pipelines.py           # 数据处理管道
├── settings.py            # 配置文件
├── scripts/               # 脚本文件
├── utils/                 # 工具模块
└── url_manager/          # URL管理模块
```

## 监控和管理

- 访问监控面板：http://localhost:8080
- 查看运行日志：logs/spider_*.log
- 数据统计：output/crawl_summary.txt

## 注意事项

1. 请遵守网站的robots协议
2. 合理控制爬取频率
3. 定期维护代理池
4. 及时备份重要数据

## 常见问题

1. Q: 如何添加新的爬虫节点？
   A: 修改配置文件中的节点数量，然后重启系统。

2. Q: 如何处理代理失效？
   A: 系统会自动检测并更换失效代理。

3. Q: 如何查看爬取进度？
   A: 通过监控面板或查看日志文件。

## 贡献指南

欢迎提交Issue和Pull Request来帮助改进项目。
