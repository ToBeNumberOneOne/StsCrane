import os
import csv
import json
import socket
import struct
import time
import logging
import threading
import sys

from datetime import datetime

def parse_data(data, bool_counts, float_counts):
    # 向上取整到 8 的整数倍
    bool_counts_padded = bool_counts + (8 - bool_counts % 8) % 8

    # 解析 bool 数据类型
    bool_bytes = data[:bool_counts_padded // 8]
    bool_data = struct.unpack(f">{bool_counts_padded // 8}B", bool_bytes)
    bool_values = [int((bool(bool_data[i // 8] & (1 << (i % 8))))) for i in range(bool_counts)]

     # 向上取整到 32 的整数倍
    bool_counts_padded = bool_counts + (32 - bool_counts % 32) % 32
    # 解析 float 数据类型
    float_bytes = data[bool_counts_padded // 8:bool_counts_padded // 8 + float_counts * 4]
    float_values = struct.unpack(f"<{float_counts}f", float_bytes)
    float_values = [round(x, 2) for x in float_values]

    # 解析时间戳
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S.%f")[:-3]
    return tuple([timestamp] + bool_values + list(float_values))

def connect_server(server_info):
    try:
        client = socket.socket()
        address = tuple(server_info['address'])
        client.settimeout(10)  # 设置超时时间为 10 秒
        client.connect(address)
        
        logging.info(f"Connected to server {server_info['address']}")
        return client
    except:
        logging.error(f"Failed to connect to server {server_info['address']}")
        time.sleep(5)
        return None
    

def receive_save_data(server, cache_size, bool_count, float_count, names):
    data_cache = []
    client = connect_server(server)
    HeartBeat = 0

    while True:
        try:
            # 发送数据
            HeartBeat = (HeartBeat + 1) % 256
            client.send(struct.pack('B', HeartBeat))
            data = client.recv(1024)
            logging.debug(f"Receive data from server {server['name']}")
            if not data:
                continue

            result = parse_data(data, bool_count, float_count)
        

            data_cache.append(result)

            if len(data_cache) >= cache_size:
                # 创建跨平台兼容的路径
                directory = os.path.join('LogDatas', server['name'])
                os.makedirs(directory, exist_ok=True)

                # 创建文件名
                timestamp = datetime.now().strftime("%Y-%m-%d %H_%M_%S")
                filename = os.path.join(directory, f"{timestamp}.csv")
                with open(filename, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(names)
                    writer.writerows(data_cache)
                # 记录写入 CSV 文件的事件
                logging.debug(f"Wrote {len(data_cache)} rows to {filename} from {server['name']}")

                data_cache = []

        except Exception as e:
            logging.error(f"Failed to receive data from server {server['address']}: {e}")
            time.sleep(10)
            client = connect_server(server)


        
        


# 读取配置文件
Config_file = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), 'Config.json')
try:
    with open(Config_file, "r") as f:
        config = json.load(f)
except Exception as e:
    logging.error(f"Failed to read configuration file {Config_file}: {e}")
    sys.exit(1)

servers = config.get("servers_list", [])
cache_size = config.get("cache_size", 100)
cache_size = max(100, min(cache_size, 10000))
log_level = config.get("log_level", "INFO")

# 配置日志记录器
log_file = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), 'SocketClient.log')

logging.basicConfig(filename=log_file, level=getattr(logging,log_level), format='%(asctime)s - [%(funcName)s-->line:%(lineno)d] - '
                                                                  '%(levelname)s:%(message)s')


# 读取DataConfig.csv文件

bool_count = 0
float_count = 0
names = ['Data&time']

# 创建跨平台兼容的路径
Data_config = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), 'DataConfig.csv')
try:
    with open(Data_config, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['type'] == 'bool':
                bool_count += 1
            if row['type'] == 'float':
                float_count += 1
            names.append(row['name'])
except Exception as e:
    logging.error(f"Failed to read configuration file {Data_config}: {e}")
    sys.exit(1)

# 启动线程
threads = []
for server in servers:
    thread = threading.Thread(target=receive_save_data, args=(server, cache_size, bool_count, float_count, names))
    thread.start()
    threads.append(thread)

# 等待所有线程结束
for thread in threads:
    thread.join()


