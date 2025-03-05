import os
import logging
import json
import pandas as pd
import sqlite3
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

class DataStorage:
    """数据存储模块，支持Parquet和SQLite存储"""
    
    def __init__(self, base_dir='data'):
        """
        初始化数据存储模块
        
        Args:
            base_dir: 数据存储的基础目录
        """
        self.logger = logging.getLogger('data_storage')
        self.logger.setLevel(logging.INFO)
        
        # 创建数据目录
        self.base_dir = base_dir
        self.parquet_dir = os.path.join(base_dir, 'parquet')
        self.sqlite_db_path = os.path.join(base_dir, 'douban.db')
        
        # 确保目录存在
        os.makedirs(self.parquet_dir, exist_ok=True)
        
        # 初始化SQLite连接
        self._init_sqlite()
        
    def _init_sqlite(self):
        """初始化SQLite数据库"""
        try:
            self.conn = sqlite3.connect(self.sqlite_db_path)
            self.cursor = self.conn.cursor()
            
            # 创建电影表
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS movies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    director TEXT,
                    screenwriter TEXT,
                    actors TEXT,
                    genres TEXT,
                    country TEXT,
                    language TEXT,
                    release_date TEXT,
                    runtime TEXT,
                    imdb TEXT,
                    rate REAL,
                    num INTEGER,
                    rank INTEGER UNIQUE,
                    crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 创建索引
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_rank ON movies(rank)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_rate ON movies(rate)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_genres ON movies(genres)')
            
            self.conn.commit()
            self.logger.info("SQLite数据库初始化成功")
            
        except Exception as e:
            self.logger.error(f"初始化SQLite数据库失败: {e}")
            raise
            
    def store_items(self, items, batch_id=None):
        """
        存储爬虫抓取的项目
        
        Args:
            items: 爬虫抓取的项目列表
            batch_id: 批次ID，默认使用当前时间戳
        """
        if not items:
            self.logger.warning("没有数据需要存储")
            return
            
        try:
            # 生成批次ID
            if batch_id is None:
                batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
                
            # 转换为DataFrame
            df = pd.DataFrame(items)
            
            # 存储为Parquet
            self._store_parquet(df, batch_id)
            
            # 存储到SQLite
            self._store_sqlite(df)
            
            self.logger.info(f"成功存储 {len(items)} 条数据，批次ID: {batch_id}")
            
        except Exception as e:
            self.logger.error(f"存储数据失败: {e}")
            raise
            
    def _store_parquet(self, df, batch_id):
        """存储数据为Parquet格式"""
        try:
            # 按日期分区存储
            date_str = datetime.now().strftime('%Y-%m-%d')
            partition_dir = os.path.join(self.parquet_dir, date_str)
            os.makedirs(partition_dir, exist_ok=True)
            
            # 文件路径
            file_path = os.path.join(partition_dir, f'douban_movies_{batch_id}.parquet')
            
            # 存储为Parquet
            table = pa.Table.from_pandas(df)
            pq.write_table(
                table, 
                file_path,
                compression='snappy'  # 使用snappy压缩
            )
            
            self.logger.info(f"数据已存储为Parquet: {file_path}")
            
        except Exception as e:
            self.logger.error(f"存储Parquet失败: {e}")
            raise
            
    def _store_sqlite(self, df):
        """存储数据到SQLite"""
        try:
            # 处理列表类型的字段
            for col in ['director', 'screenwriter', 'actors', 'genres']:
                if col in df.columns and df[col].dtype == 'object':
                    df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, list) else x)
            
            # 存储到SQLite
            df.to_sql('movies', self.conn, if_exists='append', index=False)
            self.logger.info(f"数据已存储到SQLite: {len(df)}条记录")
            
        except Exception as e:
            self.logger.error(f"存储SQLite失败: {e}")
            raise
            
    def query_movies(self, conditions=None, order_by=None, limit=100):
        """
        查询电影数据
        
        Args:
            conditions: 查询条件，字典格式 {'字段': '值'}
            order_by: 排序字段，格式 ('字段', 'asc'/'desc')
            limit: 返回记录数量限制
            
        Returns:
            DataFrame: 查询结果
        """
        try:
            query = "SELECT * FROM movies"
            params = []
            
            # 添加查询条件
            if conditions:
                where_clauses = []
                for field, value in conditions.items():
                    if isinstance(value, str) and '%' in value:
                        where_clauses.append(f"{field} LIKE ?")
                    else:
                        where_clauses.append(f"{field} = ?")
                    params.append(value)
                    
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)
            
            # 添加排序
            if order_by:
                field, direction = order_by
                query += f" ORDER BY {field} {direction}"
                
            # 添加限制
            query += f" LIMIT {limit}"
            
            # 执行查询
            df = pd.read_sql_query(query, self.conn, params=params)
            
            # 处理JSON字段
            for col in ['director', 'screenwriter', 'actors', 'genres']:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: json.loads(x) if isinstance(x, str) and x.startswith('[') else x)
                    
            return df
            
        except Exception as e:
            self.logger.error(f"查询数据失败: {e}")
            return pd.DataFrame()
            
    def get_movie_by_rank(self, rank):
        """根据排名获取电影"""
        return self.query_movies(conditions={'rank': rank}, limit=1)
        
    def get_movies_by_genre(self, genre, limit=50):
        """根据类型获取电影"""
        return self.query_movies(conditions={'genres': f'%{genre}%'}, order_by=('rate', 'desc'), limit=limit)
        
    def get_top_rated_movies(self, limit=10):
        """获取评分最高的电影"""
        return self.query_movies(order_by=('rate', 'desc'), limit=limit)
        
    def get_statistics(self):
        """获取统计信息"""
        try:
            stats = {}
            
            # 总电影数
            self.cursor.execute("SELECT COUNT(*) FROM movies")
            stats['total_movies'] = self.cursor.fetchone()[0]
            
            # 平均评分
            self.cursor.execute("SELECT AVG(rate) FROM movies")
            stats['avg_rating'] = round(self.cursor.fetchone()[0], 2)
            
            # 评分分布
            self.cursor.execute("""
                SELECT 
                    CASE 
                        WHEN rate >= 9.0 THEN '9.0-10.0'
                        WHEN rate >= 8.0 THEN '8.0-8.9'
                        WHEN rate >= 7.0 THEN '7.0-7.9'
                        WHEN rate >= 6.0 THEN '6.0-6.9'
                        ELSE '< 6.0'
                    END as rating_range,
                    COUNT(*) as count
                FROM movies
                GROUP BY rating_range
                ORDER BY MIN(rate) DESC
            """)
            stats['rating_distribution'] = dict(self.cursor.fetchall())
            
            # 类型分布
            self.cursor.execute("""
                SELECT genres, COUNT(*) as count
                FROM movies
                GROUP BY genres
                ORDER BY count DESC
                LIMIT 10
            """)
            stats['genre_distribution'] = dict(self.cursor.fetchall())
            
            return stats
            
        except Exception as e:
            self.logger.error(f"获取统计信息失败: {e}")
            return {}
            
    def export_to_excel(self, file_path='豆瓣电影.xlsx'):
        """导出数据到Excel"""
        try:
            df = self.query_movies(limit=1000)  # 最多导出1000条
            df.to_excel(file_path, index=False)
            self.logger.info(f"数据已导出到Excel: {file_path}")
            return True
        except Exception as e:
            self.logger.error(f"导出Excel失败: {e}")
            return False
            
    def close(self):
        """关闭连接"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            self.logger.info("数据库连接已关闭")

# 单例模式
_data_storage = None

def get_data_storage():
    """获取数据存储单例"""
    global _data_storage
    if _data_storage is None:
        _data_storage = DataStorage()
    return _data_storage

if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 创建测试数据
    test_items = [
        {
            'name': '肖申克的救赎',
            'director': ['弗兰克·德拉邦特'],
            'actors': ['蒂姆·罗宾斯', '摩根·弗里曼'],
            'genres': ['剧情', '犯罪'],
            'country': '美国',
            'rate': 9.7,
            'num': 2500000,
            'rank': 1
        },
        {
            'name': '霸王别姬',
            'director': ['陈凯歌'],
            'actors': ['张国荣', '巩俐'],
            'genres': ['剧情', '爱情'],
            'country': '中国大陆',
            'rate': 9.6,
            'num': 1800000,
            'rank': 2
        }
    ]
    
    # 测试存储
    storage = get_data_storage()
    storage.store_items(test_items)
    
    # 测试查询
    print("按排名查询:")
    print(storage.get_movie_by_rank(1))
    
    print("\n按类型查询:")
    print(storage.get_movies_by_genre('剧情'))
    
    print("\n评分最高的电影:")
    print(storage.get_top_rated_movies())
    
    print("\n统计信息:")
    print(storage.get_statistics())
    
    # 关闭连接
    storage.close()