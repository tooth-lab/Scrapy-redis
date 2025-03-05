import sqlite3
import pandas as pd
import os
import argparse
from prettytable import PrettyTable

class MovieQuery:
    def __init__(self,db_path):
        self.db_path = r'C:\Users\terry\OneDrive\桌面\爬虫与分布式爬虫\3.5-第一\Douban2\output\movies.db'
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"数据库文件 {self.db_path} 不存在")
            
    def execute_query(self, sql, params=None):
        """执行SQL查询"""
        conn = sqlite3.connect(self.db_path)
        try:
            df = pd.read_sql_query(sql, conn, params=params)
            return df
        finally:
            conn.close()
            
    def print_results(self, df, max_rows=20):
        """打印查询结果"""
        if df.empty:
            print("没有找到匹配的记录")
            return
            
        # 创建PrettyTable
        pt = PrettyTable()
        pt.field_names = df.columns.tolist()
        
        # 添加行
        for _, row in df.head(max_rows).iterrows():
            pt.add_row(row.tolist())
            
        print(pt)
        
        if len(df) > max_rows:
            print(f"\n... 还有 {len(df) - max_rows} 条记录未显示")
            
    def search_by_keyword(self):
        """关键词搜索"""
        keyword = input("请输入搜索关键词: ")
        sql = """
        SELECT name, director, screenwriter, actors, rate, num
        FROM movies_all
        WHERE name LIKE ? 
           OR director LIKE ?
           OR screenwriter LIKE ?
           OR actors LIKE ?
        ORDER BY rate DESC
        """
        params = [f"%{keyword}%"] * 4
        df = self.execute_query(sql, params)
        self.print_results(df)
        
    def search_by_date_range(self):
        """时间范围查询"""
        start_year = input("请输入起始年份: ")
        end_year = input("请输入结束年份: ")
        sql = """
        SELECT name, director, release_date, rate
        FROM movies_all
        WHERE CAST(SUBSTR(release_date, 1, 4) AS INTEGER) BETWEEN ? AND ?
        ORDER BY release_date DESC
        """
        df = self.execute_query(sql, [start_year, end_year])
        self.print_results(df)
        
    def search_by_genre(self):
        """分类查询"""
        genre = input("请输入电影类型: ")
        sql = """
        SELECT name, director, genres, rate, num
        FROM movies_all
        WHERE genres LIKE ?
        ORDER BY rate DESC
        """
        df = self.execute_query(sql, [f"%{genre}%"])
        self.print_results(df)
        
    def show_statistics(self):
        """统计查询菜单"""
        queries = {
            1: ("基础数量统计", """
                SELECT 
                    COUNT(*) as total_movies,
                    COUNT(DISTINCT director) as total_directors,
                    COUNT(DISTINCT genres) as total_genres,
                    ROUND(AVG(rate), 2) as avg_rate,
                    ROUND(AVG(num), 0) as avg_votes
                FROM movies_all
                """),
            2: ("分组统计", """
                SELECT 
                    SUBSTR(release_date, 1, 4) as year,
                    COUNT(*) as movie_count,
                    ROUND(AVG(rate), 2) as avg_rate,
                    ROUND(AVG(num), 0) as avg_votes,
                    MAX(rate) as highest_rate,
                    MIN(rate) as lowest_rate
                FROM movies_all
                GROUP BY SUBSTR(release_date, 1, 4)
                ORDER BY year DESC
                """),
            3: ("电影时长分布", """
                SELECT 
                    CASE 
                        WHEN runtime <= 90 THEN '90分钟以下'
                        WHEN runtime <= 120 THEN '90-120分钟'
                        WHEN runtime <= 150 THEN '120-150分钟'
                        ELSE '150分钟以上'
                    END as duration_range,
                    COUNT(*) as movie_count,
                    ROUND(AVG(rate), 2) as avg_rate
                FROM movies_all 
                GROUP BY duration_range
                ORDER BY movie_count DESC
                """),
            4: ("国家/地区分布", """
                SELECT 
                    country,
                    COUNT(*) as movie_count,
                    ROUND(AVG(rate), 2) as avg_rate
                FROM movies_all 
                GROUP BY country
                ORDER BY movie_count DESC
                """)
        }
        
        while True:
            print("\n=== 统计查询 ===")
            for key, (desc, _) in queries.items():
                print(f"{key}. {desc}")
            print("0. 返回上级菜单")
            
            choice = input("\n请选择查询类型 (0-4): ")
            if choice == '0':
                break
                
            try:
                choice = int(choice)
                if choice not in queries:
                    print("无效的选择")
                    continue
                    
                desc, sql = queries[choice]
                df = self.execute_query(sql)
                print(f"\n{desc}结果:")
                self.print_results(df)
                
            except ValueError:
                print("请输入有效的数字")
                
    def trend_analysis(self):
        """趋势分析菜单"""
        queries = {
            1: ("评分趋势", """
                SELECT 
                    SUBSTR(release_date, 1, 4) as year,
                    COUNT(*) as movie_count,
                    ROUND(AVG(rate), 2) as avg_rate,
                    ROUND(AVG(num), 0) as avg_votes,
                    SUM(CASE WHEN rate >= 9.0 THEN 1 ELSE 0 END) as high_rated_count
                FROM movies_all
                GROUP BY SUBSTR(release_date, 1, 4)
                HAVING movie_count >= 3
                ORDER BY year
                """),
            2: ("观众人数趋势", """
                SELECT 
                    SUBSTR(release_date, 1, 4) as year,
                    ROUND(AVG(num), 0) as avg_votes,
                    MAX(num) as max_votes,
                    MIN(num) as min_votes
                FROM movies_all
                GROUP BY SUBSTR(release_date, 1, 4)
                ORDER BY year
                """),
            3: ("电影类型变化", """
                SELECT 
                    CASE 
                        WHEN CAST(SUBSTR(release_date, 1, 4) AS INTEGER) < 1980 THEN '1980年前'
                        WHEN CAST(SUBSTR(release_date, 1, 4) AS INTEGER) BETWEEN 1980 AND 1989 THEN '1980年代'
                        WHEN CAST(SUBSTR(release_date, 1, 4) AS INTEGER) BETWEEN 1990 AND 1999 THEN '1990年代'
                        WHEN CAST(SUBSTR(release_date, 1, 4) AS INTEGER) BETWEEN 2000 AND 2009 THEN '2000年代'
                        ELSE '2010年后'
                    END as period,
                    genres,
                    COUNT(*) as movie_count,
                    ROUND(AVG(rate), 2) as avg_rate
                FROM movies_all
                GROUP BY period, genres
                ORDER BY period, movie_count DESC
                """)
        }
        
        while True:
            print("\n=== 趋势分析 ===")
            for key, (desc, _) in queries.items():
                print(f"{key}. {desc}")
            print("0. 返回上级菜单")
            
            choice = input("\n请选择分析类型 (0-3): ")
            if choice == '0':
                break
                
            try:
                choice = int(choice)
                if choice not in queries:
                    print("无效的选择")
                    continue
                    
                desc, sql = queries[choice]
                df = self.execute_query(sql)
                print(f"\n{desc}结果:")
                self.print_results(df)
                
            except Exception as e:
                print(f"分析出错: {e}")
                
    def advanced_analysis(self):
        """高级分析菜单"""
        queries = {
            1: ("高分电影分析", """
                SELECT name, director, rate, runtime, num
                FROM movies_all
                WHERE rate >= 8.5 
                  AND CAST(REPLACE(runtime, '分钟', '') AS INTEGER) >= 150
                  AND num >= 10000
                ORDER BY rate DESC
                """),
            2: ("导演作品分析", """
                SELECT 
                    director,
                    COUNT(*) as movie_count,
                    ROUND(AVG(rate), 2) as avg_rate,
                    ROUND(AVG(num), 0) as avg_votes,
                    GROUP_CONCAT(DISTINCT genres) as all_genres,
                    MAX(rate) as best_rate,
                    MIN(rate) as worst_rate
                FROM movies_all
                GROUP BY director
                HAVING movie_count >= 2
                ORDER BY avg_rate DESC
                """),
            3: ("评分区间分析", """
                SELECT 
                    CASE 
                        WHEN rate >= 9.0 THEN '9分以上'
                        WHEN rate >= 8.5 THEN '8.5-9分'
                        WHEN rate >= 8.0 THEN '8-8.5分'
                        WHEN rate >= 7.5 THEN '7.5-8分'
                        ELSE '7.5分以下'
                    END as rate_range,
                    COUNT(*) as movie_count,
                    ROUND(AVG(num), 0) as avg_votes,
                    GROUP_CONCAT(DISTINCT SUBSTR(genres, 1, INSTR(genres, '/')-1)) as main_genres
                FROM movies_all
                GROUP BY rate_range
                ORDER BY rate_range DESC
                """),
            4: ("数据质量分析", """
                SELECT 
                    COUNT(*) as total_movies,
                    SUM(CASE WHEN rate >= 9.0 THEN 1 ELSE 0 END) as high_rated_count,
                    SUM(CASE WHEN num >= 100000 THEN 1 ELSE 0 END) as popular_count,
                    SUM(CASE WHEN runtime IS NULL OR runtime = '' THEN 1 ELSE 0 END) as missing_runtime,
                    SUM(CASE WHEN genres IS NULL OR genres = '' THEN 1 ELSE 0 END) as missing_genres,
                    SUM(CASE WHEN release_date IS NULL OR release_date = '' THEN 1 ELSE 0 END) as missing_date
                FROM movies_all
                """)
        }
        
        while True:
            print("\n=== 高级分析 ===")
            for key, (desc, _) in queries.items():
                print(f"{key}. {desc}")
            print("0. 返回上级菜单")
            
            choice = input("\n请选择分析类型 (0-4): ")
            if choice == '0':
                break
                
            try:
                choice = int(choice)
                if choice not in queries:
                    print("无效的选择")
                    continue
                    
                desc, sql = queries[choice]
                df = self.execute_query(sql)
                print(f"\n{desc}结果:")
                self.print_results(df)
                
            except Exception as e:
                print(f"分析出错: {e}")

    def custom_query(self):
        """自定义SQL查询"""
        print("\n=== 自定义SQL查询 ===")
        print("请输入SQL查询语句 (输入 'exit' 返回主菜单):")
        
        while True:
            sql = input("\nSQL> ")
            if sql.lower() == 'exit':
                break
                
            try:
                df = self.execute_query(sql)
                self.print_results(df)
            except Exception as e:
                print(f"查询出错: {e}")
                
    def export_results(self, df, filename):
        """导出查询结果"""
        if df.empty:
            print("没有数据可导出")
            return
            
        ext = filename.split('.')[-1].lower()
        if ext == 'csv':
            df.to_csv(filename, index=False, encoding='utf-8-sig')
        elif ext == 'xlsx':
            df.to_excel(filename, index=False)
        elif ext == 'json':
            df.to_json(filename, orient='records', force_ascii=False)
        else:
            print("不支持的文件格式")
            return
            
        print(f"数据已导出到: {filename}")

def main():
    parser = argparse.ArgumentParser(description='豆瓣电影数据查询工具')
    parser.add_argument('--db', default='movies.db', help='数据库文件路径')
    args = parser.parse_args()
    
    try:
        query = MovieQuery(args.db)
        
        while True:
            print("\n=== 豆瓣电影数据查询系统 ===")
            print("1. 关键词搜索")
            print("2. 时间范围查询")
            print("3. 分类查询")
            print("4. 统计查询")
            print("5. 趋势分析")
            print("6. 高级分析")
            print("7. 自定义SQL查询")
            print("0. 退出")
            
            choice = input("\n请选择功能 (0-7): ")
            
            if choice == '0':
                break
            elif choice == '1':
                query.search_by_keyword()
            elif choice == '2':
                query.search_by_date_range()
            elif choice == '3':
                query.search_by_genre()
            elif choice == '4':
                query.show_statistics()
            elif choice == '5':
                query.trend_analysis()
            elif choice == '6':
                query.advanced_analysis()
            elif choice == '7':
                query.custom_query()
            else:
                print("无效的选择")
                
    except Exception as e:
        print(f"程序错误: {e}")

if __name__ == "__main__":
    main() 