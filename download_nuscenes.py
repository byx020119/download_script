import os
import requests
from tqdm import tqdm
import argparse
import threading
from queue import Queue
import time

def download_file(url, save_dir, chunk_size=1024*1024):
    """
    下载文件并显示进度，支持断点续传
    
    参数:
        url: 文件下载URL
        save_dir: 保存目录
        chunk_size: 下载块大小，默认为1MB
    """
    # 创建保存目录
    os.makedirs(save_dir, exist_ok=True)
    
    # 获取文件名
    filename = url.split('/')[-1]
    save_path = os.path.join(save_dir, filename)
    
    # 检查文件是否已部分下载
    resume_byte_pos = 0
    if os.path.exists(save_path):
        resume_byte_pos = os.path.getsize(save_path)
        print(f"发现部分下载的文件 {filename}，将从 {resume_byte_pos} 字节处继续下载")
    
    # 设置请求头，支持断点续传
    headers = {}
    if resume_byte_pos > 0:
        headers['Range'] = f'bytes={resume_byte_pos}-'
    
    try:
        # 发送请求
        with requests.get(url, headers=headers, stream=True, timeout=30) as r:
            r.raise_for_status()
            
            # 获取文件总大小
            total_size = int(r.headers.get('content-length', 0)) + resume_byte_pos
            
            # 显示下载进度
            with open(save_path, 'ab') as f, tqdm(
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                initial=resume_byte_pos,
                desc=filename
            ) as pbar:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:  # 过滤掉保持连接的空块
                        f.write(chunk)
                        pbar.update(len(chunk))
        
        print(f"文件 {filename} 下载完成！")
        return True
    
    except requests.exceptions.RequestException as e:
        print(f"下载 {filename} 时出错: {e}")
        return False

def worker(queue, save_dir, failed_queue):
    """工作线程函数，从队列中获取URL并下载"""
    while not queue.empty():
        url = queue.get()
        try:
            success = download_file(url, save_dir)
            if not success:
                failed_queue.put(url)
        finally:
            queue.task_done()

def download_files_in_parallel(urls, save_dir, max_threads=5):
    """
    并行下载多个文件
    
    参数:
        urls: 要下载的URL列表
        save_dir: 保存目录
        max_threads: 最大线程数，控制并行下载数量
    """
    # 创建队列并填充URL
    queue = Queue()
    for url in urls:
        queue.put(url)
    
    # 用于存储下载失败的URL
    failed_queue = Queue()
    
    # 创建并启动工作线程
    threads = []
    thread_count = min(max_threads, len(urls))
    print(f"将使用 {thread_count} 个线程进行并行下载...")
    
    for _ in range(thread_count):
        t = threading.Thread(target=worker, args=(queue, save_dir, failed_queue))
        t.start()
        threads.append(t)
    
    # 等待所有线程完成
    queue.join()
    
    # 收集下载失败的URL
    failed_urls = []
    while not failed_queue.empty():
        failed_urls.append(failed_queue.get())
    
    return failed_urls

def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='多线程下载nuScenes数据集工具')
    parser.add_argument('--save-dir', type=str, default='nuscenes', 
                      help='数据集保存目录，默认为当前目录下的nuscenes')
    parser.add_argument('--threads', type=int, default=3, 
                      help='并行下载的线程数，默认为3')
    args = parser.parse_args()
    
    # nuScenes完整数据集的11个tgz文件URL
    urls = [
        "https://motional-nuscenes.s3.amazonaws.com/public/v1.0/v1.0-trainval01_blobs.tgz",
        "https://motional-nuscenes.s3.amazonaws.com/public/v1.0/v1.0-trainval02_blobs.tgz",
        "https://motional-nuscenes.s3.amazonaws.com/public/v1.0/v1.0-trainval03_blobs.tgz",
        "https://motional-nuscenes.s3.amazonaws.com/public/v1.0/v1.0-trainval04_blobs.tgz",
        "https://motional-nuscenes.s3.amazonaws.com/public/v1.0/v1.0-trainval05_blobs.tgz",
        "https://motional-nuscenes.s3.amazonaws.com/public/v1.0/v1.0-trainval06_blobs.tgz",
        "https://motional-nuscenes.s3.amazonaws.com/public/v1.0/v1.0-trainval07_blobs.tgz",
        "https://motional-nuscenes.s3.amazonaws.com/public/v1.0/v1.0-trainval08_blobs.tgz",
        "https://motional-nuscenes.s3.amazonaws.com/public/v1.0/v1.0-trainval09_blobs.tgz",
        "https://motional-nuscenes.s3.amazonaws.com/public/v1.0/v1.0-trainval10_blobs.tgz",
        "https://d36yt3mvayqw5m.cloudfront.net/public/v1.0/v1.0-trainval_meta.tgz"
    ]
    
    print(f"开始下载nuScenes数据集，共 {len(urls)} 个文件...")
    print(f"文件将保存到: {os.path.abspath(args.save_dir)}")
    print(f"并行下载线程数: {args.threads}")
    
    # 首次下载
    failed_urls = download_files_in_parallel(urls, args.save_dir, args.threads)
    
    # 重试下载失败的文件
    retry_count = 3
    for i in range(retry_count):
        if not failed_urls:
            break
            
        print(f"\n第 {i+1} 次重试下载失败的 {len(failed_urls)} 个文件...")
        failed_urls = download_files_in_parallel(failed_urls, args.save_dir, args.threads)
        time.sleep(2)  # 重试前稍等片刻
    
    if failed_urls:
        print(f"\n以下 {len(failed_urls)} 个文件下载失败:")
        for url in failed_urls:
            print(f"- {url}")
    else:
        print("\n所有文件均下载成功！")
    
    print("\n下载操作已完成！")

if __name__ == "__main__":
    main()
    