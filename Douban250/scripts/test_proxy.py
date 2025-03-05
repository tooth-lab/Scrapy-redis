import sys
import os
import time
import logging
import requests
import json
from pathlib import Path

# 添加项目根目录到Python路径
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from scrapy.utils.project import get_project_settings
from utils.proxy_pool import get_proxy_pool

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('proxy_test.log')
    ]
)

logger = logging.getLogger(__name__)

def get_real_ip():
    """获取当前IP信息"""
    try:
        response = requests.get('https://httpbin.org/ip', timeout=10)
        if response.status_code == 200:
            return response.json().get('origin')
    except Exception as e:
        logger.error(f"获取本地IP失败: {str(e)}")
    return None

def test_proxy_availability(proxy, test_urls):
    """测试代理可用性"""
    results = []
    
    # 首先测试IP
    try:
        start_time = time.time()
        response = requests.get(
            'https://httpbin.org/ip',
            proxies={'https': proxy},
            timeout=10
        )
        response_time = time.time() - start_time
        
        if response.status_code == 200:
            proxy_ip = response.json().get('origin')
            logger.info(f"代理IP检测: {proxy_ip}")
            
            # 验证是否是中国IP
            ip_info_response = requests.get(f'https://ipapi.co/{proxy_ip}/json/', timeout=10)
            if ip_info_response.status_code == 200:
                ip_info = ip_info_response.json()
                logger.info(f"IP归属地: {ip_info.get('country_name')} - {ip_info.get('city')}")
    except Exception as e:
        logger.error(f"IP检测失败: {str(e)}")
    
    # 测试目标URL
    for url in test_urls:
        try:
            start_time = time.time()
            response = requests.get(
                url,
                proxies={'https': proxy},
                timeout=10,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br'
                }
            )
            response_time = time.time() - start_time
            
            # 获取实际IP
            real_ip = None
            try:
                ip_check = requests.get(
                    'https://httpbin.org/ip',
                    proxies={'https': proxy},
                    timeout=5
                )
                if ip_check.status_code == 200:
                    real_ip = ip_check.json().get('origin')
            except:
                pass
            
            result = {
                'url': url,
                'status_code': response.status_code,
                'response_time': response_time,
                'success': response.status_code in [200, 403, 418],
                'ip': real_ip or '未知',
                'content_length': len(response.content),
                'headers': dict(response.headers)
            }
            
            results.append(result)
            logger.info(f"测试 {url}:")
            logger.info(f"  状态码: {result['status_code']}")
            logger.info(f"  响应时间: {result['response_time']:.2f}秒")
            logger.info(f"  代理IP: {result['ip']}")
            logger.info(f"  响应大小: {result['content_length']/1024:.2f}KB")
            
            # 测试间隔
            time.sleep(5)
            
        except Exception as e:
            logger.error(f"测试 {url} 失败: {str(e)}")
            results.append({
                'url': url,
                'status_code': None,
                'response_time': None,
                'success': False,
                'ip': '未知',
                'error': str(e)
            })
            
    return results

def analyze_results(results):
    """分析测试结果"""
    total_tests = len(results)
    successful_tests = sum(1 for r in results if r['success'])
    avg_response_time = sum(r['response_time'] for r in results if r['response_time']) / total_tests
    
    logger.info("\n测试结果分析:")
    logger.info(f"总测试次数: {total_tests}")
    logger.info(f"成功次数: {successful_tests}")
    logger.info(f"成功率: {(successful_tests/total_tests)*100:.2f}%")
    logger.info(f"平均响应时间: {avg_response_time:.2f}秒")
    
    # 检查IP是否变化
    ips = set(r['ip'] for r in results if r['ip'] != '未知')
    logger.info(f"使用的IP数量: {len(ips)}")
    if ips:
        logger.info("IP列表:")
        for ip in ips:
            logger.info(f"  - {ip}")

def main():
    """主函数"""
    settings = get_project_settings()
    proxy_pool = get_proxy_pool()
    
    # 获取本地IP
    local_ip = get_real_ip()
    if local_ip:
        logger.info(f"本地IP: {local_ip}")
    
    test_urls = [
        'https://movie.douban.com/robots.txt',
        'https://movie.douban.com/',
        'https://movie.douban.com/top250'
    ]
    
    logger.info("开始代理测试...")
    
    # 获取并测试代理
    proxy = proxy_pool.get_proxy()
    if not proxy:
        logger.error("无法获取代理")
        return
        
    logger.info(f"获取到代理: {proxy}")
    
    # 运行测试
    results = test_proxy_availability(proxy, test_urls)
    
    # 分析结果
    analyze_results(results)
    
    # 更新代理状态
    success_rate = sum(1 for r in results if r['success']) / len(results)
    avg_response_time = sum(r['response_time'] for r in results if r['response_time']) / len(results)
    
    if success_rate >= 0.7:  # 如果成功率超过70%
        proxy_pool.report_proxy_status(proxy, success=True, response_time=avg_response_time)
        logger.info("代理测试通过")
    else:
        proxy_pool.report_proxy_status(proxy, success=False)
        logger.warning("代理测试未通过")

if __name__ == '__main__':
    main() 