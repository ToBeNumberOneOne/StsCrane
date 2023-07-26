import os
import csv
import json
import socket
import struct
import time
from datetime import datetime

import logging
import threading
import sys


# 创建跨平台兼容的路径
log_file = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), 'SocketClient.log')

# 配置日志记录器
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - [%(funcName)s-->line:%(lineno)d] - '
                                                                  '%(levelname)s:%(message)s')


def parse_data(data, bool_counts, float_counts):
    # 解析 bool 数据类型
    bool_bytes = data[:bool_counts // 8]
    bool_data = struct.unpack(f">{bool_counts // 8}B", bool_bytes)
    bool_values = [int((bool(bool_data[i // 8] & (1 << (i % 8))))) for i in range(bool_counts)]

    # 解析 float 数据类型
    float_bytes = data[bool_counts // 8:bool_counts // 8 + float_counts * 4]
    float_values = struct.unpack(f"<{float_counts}f", float_bytes)
    float_values = [round(x, 2) for x in float_values]

    # 解析时间戳
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S.%f")[:-3]
    return tuple([timestamp] + bool_values + list(float_values))


def connect_server(server_info):
    try:
        client = socket.socket()
        client.connect(server_info['address'])
        logging.info(f"Connected to server {server_info['address']}")
        return client
    except Exception as e:
        logging.error(f"Failed to connect to server {server_info['address']}: {e}")
        time.sleep(10)


def receive_save_data(server, bool_count, float_count, names):
    data_cache = []
    client = connect_server(server)

    while True:
        try:
            data = client.recv(2048)
            if not data:
                continue

            result = parse_data(data, bool_count, float_count)

            data_cache.append(result)

            if len(data_cache) >= 20:
                # 创建跨平台兼容的路径
                directory = os.path.join('data', server['name'])
                os.makedirs(directory, exist_ok=True)

                # 创建文件名
                timestamp = datetime.now().strftime("%Y-%m-%d %H_%M_%S")
                filename = os.path.join(directory, f"{timestamp}.csv")
                with open(filename, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(names)
                    writer.writerows(data_cache)
                # 记录写入 CSV 文件的事件
                logging.info(f"Wrote {len(data_cache)} rows to {filename} by {server['address']}")

                data_cache = []

        except Exception as e:
            logging.error(f"Failed to receive data from server {server['address']}: {e}")
            time.sleep(10)
            client = connect_server(server)

        # 记录数据接收的事件
        # logging.info(f"Received data from server {server_address}")


bool_count = 0
float_count = 0
names = ['Data&time']

# 创建跨平台兼容的路径
config_file = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), 'config.csv')

with open(config_file, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        if row['type'] == 'bool':
            bool_count += 1
        if row['type'] == 'float':
            float_count += 1
        names.append(row['name'])

servers_config = os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])), 'servers.json')

with open(servers_config,"r") as f:
    servers = json.load(f)

# 启动线程
threads = []
for server in servers:
    thread = threading.Thread(target=receive_save_data, args=(server, bool_count, float_count, names))
    thread.start()
    threads.append(thread)

# 等待所有线程结束
for thread in threads:
    thread.join()


