-- 基础查询
-- 1. 按评分降序查询所有电影
SELECT name, director, rate, num 
FROM movies_all 
ORDER BY rate DESC;

-- 2. 查询特定导演的电影
SELECT name, rate, release_date 
FROM movies_all 
WHERE director LIKE '%宫崎骏%';

-- 3. 查询评分大于9分的电影
SELECT name, director, rate, num 
FROM movies_all 
WHERE rate > 9.0 
ORDER BY rate DESC;

-- 高级查询
-- 1. 按年代统计电影数量和平均评分
SELECT 
    SUBSTR(release_date, 1, 4) as year,
    COUNT(*) as movie_count,
    ROUND(AVG(rate), 2) as avg_rate
FROM movies_all 
GROUP BY SUBSTR(release_date, 1, 4)
ORDER BY year;

-- 2. 导演作品数量和平均评分统计
SELECT 
    director,
    COUNT(*) as movie_count,
    ROUND(AVG(rate), 2) as avg_rate,
    MAX(rate) as highest_rate
FROM movies_all 
GROUP BY director
HAVING movie_count > 1
ORDER BY avg_rate DESC;

-- 3. 按类型分析电影评分
SELECT 
    genres,
    COUNT(*) as movie_count,
    ROUND(AVG(rate), 2) as avg_rate,
    ROUND(AVG(num), 0) as avg_votes
FROM movies_all 
GROUP BY genres
ORDER BY avg_rate DESC;

-- 4. 查找评分最高的前10部电影详细信息
SELECT 
    name,
    director,
    rate,
    num,
    release_date,
    genres
FROM movies_all 
ORDER BY rate DESC, num DESC
LIMIT 10;

-- 5. 电影时长分布分析
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
ORDER BY movie_count DESC;

-- 6. 国家/地区分布
SELECT 
    country,
    COUNT(*) as movie_count,
    ROUND(AVG(rate), 2) as avg_rate
FROM movies_all 
GROUP BY country
ORDER BY movie_count DESC;

-- 复杂查询部分
-- 1. 关键词搜索
-- 在电影名称、导演、编剧、演员中搜索关键词
SELECT name, director, screenwriter, actors, rate, num
FROM movies_all
WHERE name LIKE '%黑客帝国%' 
   OR director LIKE '%陈%'
   OR screenwriter LIKE '%J·K·罗琳%'
   OR actors LIKE '%周星驰%'
ORDER BY rate DESC;

-- 2. 时间范围查询
-- 查询特定年份范围的电影
SELECT name, director, release_date, rate
FROM movies_all
WHERE CAST(SUBSTR(release_date, 1, 4) AS INTEGER) BETWEEN 2000 AND 2025
ORDER BY release_date DESC;

-- 3. 分类查询
-- 按电影类型查询，支持多个类型组合
SELECT name, director, genres, rate, num
FROM movies_all
WHERE genres LIKE '%爱情%'
ORDER BY rate DESC;

-- 4. 统计查询
-- 4.1 基础数量统计
SELECT 
    COUNT(*) as total_movies,
    COUNT(DISTINCT director) as total_directors,
    COUNT(DISTINCT genres) as total_genres,
    ROUND(AVG(rate), 2) as avg_rate,
    ROUND(AVG(num), 0) as avg_votes
FROM movies_all;

-- 4.2 分组统计
SELECT 
    SUBSTR(release_date, 1, 4) as year,
    COUNT(*) as movie_count,
    ROUND(AVG(rate), 2) as avg_rate,
    ROUND(AVG(num), 0) as avg_votes,
    MAX(rate) as highest_rate,
    MIN(rate) as lowest_rate
FROM movies_all
GROUP BY SUBSTR(release_date, 1, 4)
ORDER BY year DESC;

-- 5. 趋势分析
-- 5.1 评分趋势
SELECT 
    SUBSTR(release_date, 1, 4) as year,
    COUNT(*) as movie_count,
    ROUND(AVG(rate), 2) as avg_rate,
    ROUND(AVG(num), 0) as avg_votes,
    SUM(CASE WHEN rate >= 9.0 THEN 1 ELSE 0 END) as high_rated_count
FROM movies_all
GROUP BY SUBSTR(release_date, 1, 4)
HAVING movie_count >= 3  -- 确保每年至少有3部电影
ORDER BY year;

-- 5.2 观众人数趋势
SELECT 
    SUBSTR(release_date, 1, 4) as year,
    ROUND(AVG(num), 0) as avg_votes,
    MAX(num) as max_votes,
    MIN(num) as min_votes
FROM movies_all
GROUP BY SUBSTR(release_date, 1, 4)
ORDER BY year;

-- 6. 复合查询
-- 6.1 高分电影的类型分布
SELECT 
    genres,
    COUNT(*) as movie_count,
    ROUND(AVG(rate), 2) as avg_rate,
    ROUND(AVG(num), 0) as avg_votes
FROM movies_all
WHERE rate >= 9.0
GROUP BY genres
ORDER BY movie_count DESC;

-- 6.2 不同时期的电影类型变化
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
ORDER BY period, movie_count DESC;

-- 7. 多条件组合查询
-- 7.1 高分长片分析
SELECT name, director, rate, runtime, num
FROM movies_all
WHERE rate >= 8.5 
  AND CAST(REPLACE(runtime, '分钟', '') AS INTEGER) >= 150
  AND num >= 10000
ORDER BY rate DESC;

-- 7.2 特定类型的高分电影
SELECT name, director, rate, genres, num
FROM movies_all
WHERE genres LIKE '%剧情%'
  AND rate >= 8.5
  AND num >= 50000
  AND SUBSTR(release_date, 1, 4) >= '2000'
ORDER BY rate DESC;

-- 8. 聚合查询
-- 8.1 导演作品分析
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
ORDER BY avg_rate DESC;

-- 8.2 评分区间分析
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
ORDER BY rate_range DESC;

-- 9. 数据质量分析
SELECT 
    COUNT(*) as total_movies,
    SUM(CASE WHEN rate >= 9.0 THEN 1 ELSE 0 END) as high_rated_count,
    SUM(CASE WHEN num >= 100000 THEN 1 ELSE 0 END) as popular_count,
    SUM(CASE WHEN runtime IS NULL OR runtime = '' THEN 1 ELSE 0 END) as missing_runtime,
    SUM(CASE WHEN genres IS NULL OR genres = '' THEN 1 ELSE 0 END) as missing_genres,
    SUM(CASE WHEN release_date IS NULL OR release_date = '' THEN 1 ELSE 0 END) as missing_date
FROM movies_all; 