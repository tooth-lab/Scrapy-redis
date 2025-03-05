import os
import sys
import time
import subprocess
import logging
from threading import Thread

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_command(command, wait=True):
    """运行命令"""
    logger.info(f"执行命令: {command}")
    try:
        if wait:
            subprocess.run(command, shell=True, check=True)
        else:
            subprocess.Popen(command, shell=True)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"命令执行失败: {e}")
        return False

def start_component(component_name, command):
    """启动组件"""
    logger.info(f"正在启动 {component_name}...")
    success = run_command(command, wait=False)
    if success:
        logger.info(f"{component_name} 启动成功")
    else:
        logger.error(f"{component_name} 启动失败")
    return success

def main():
    # 1. 启动URL分发器
    logger.info("步骤1: 启动URL分发器")
    dispatcher_thread = Thread(
        target=start_component,
        args=("URL分发器", "python url_dispatcher/url_dispatcher.py")
    )
    dispatcher_thread.start()
    time.sleep(2)  # 等待分发器启动

    # 2. 启动监控系统
    logger.info("步骤2: 启动监控系统")
    monitor_thread = Thread(
        target=start_component,
        args=("监控系统", "python utils/monitor.py")
    )
    monitor_thread.start()
    time.sleep(2)  # 等待监控系统启动

    # 3. 添加初始URL
    logger.info("步骤3: 添加初始URL")
    if not run_command("python scripts/add_urls_to_kafka.py"):
        return

    # 5. 启动爬虫节点
    logger.info("步骤4: 启动爬虫节点")
    nodes = []
    for i in range(3):  # 启动3个节点
        node_id = f"douban{i+1}"
        node_thread = Thread(
            target=start_component,
            args=(f"爬虫节点{i+1}", f"python scripts/run_crawler.py --node-id {node_id}")
        )
        node_thread.start()
        nodes.append(node_thread)
        time.sleep(2)  # 错开节点启动时间

    try:
        # 等待所有线程完成
        for thread in [dispatcher_thread, monitor_thread] + nodes:
            thread.join()
    except KeyboardInterrupt:
        logger.info("正在关闭所有组件...")
        # 这里可以添加优雅关闭的代码
        sys.exit(0)

if __name__ == "__main__":
    main() 