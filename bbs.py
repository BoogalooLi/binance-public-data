"""
https://github.com/binance/binance-public-data
参考这个
"""
import os
import time
import zipfile
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
import requests


# 修饰器
def cal_time(func):
    def _wrapper(*args, **kwargs):
        _start_time = time.time()
        result = func(*args, **kwargs)
        _end_time = time.time()
        print(f'{func.__name__}() 耗时 {round(_end_time - _start_time)}s\n')
        return result

    return _wrapper


# 获取下载的url
def generate_url():
    base_url = 'https://data.binance.vision/data/spot/monthly/klines'

    symbols = ["EOSUSDT", "BTCUSDT", "LTCUSDT", "ETHUSDT"]
    intervals = ["1m"]
    years = ["2017", "2018", "2019", "2020", "2021"]
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]

    urls = []
    for symbol in symbols:
        for interval in intervals:
            for year in years:
                for month in months:
                    url = f"{base_url}/{symbol}/{interval}/{symbol}-{interval}-{year}-{month}.zip"
                    urls.append(url)

    return urls


# 下载
def proceed_download(url):
    try:
        res = requests.get(url)
        res.raise_for_status()
        new_path = os.path.join(filepath, url.split("/")[-1])
        with open(new_path, 'wb') as f:
            f.write(res.content)
    except requests.HTTPError as e:
        print(f'{url}  文件不存在')
    except Exception as e:
        print(e)
        print(f'{url}  ==========下载失败！==========')
    finally:
        time.sleep(1)


# 多进程下载
@cal_time
def main_download():
    urls = generate_url()
    with ProcessPoolExecutor(max_workers=cpus) as pool:
        pool.map(proceed_download, urls)
    pool.shutdown(True)


# 获取指定目录和扩展名的所有文件列表
def get_file_list(path, extension='.csv'):
    stock_list = []
    for root, dirs, files in os.walk(path):
        if files:
            for f in files:
                if f.endswith(extension):
                    stock_list.append(os.path.join(path, f))

    return sorted(stock_list)


# 清洗数据
def clear_data(filepath):
    #  k线 12个字段，参考上面的github地址，里面有说明
    df = pd.read_csv(filepath, header=None, names=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
    if df.empty:
        pass
    df.rename(columns={0: 'MTS', 1: 'open', 2: 'high',
                       3: 'low', 4: 'close', 5: 'volume'}, inplace=True)  # 重命名
    df['candle_begin_time'] = pd.to_datetime(df['MTS'], unit='ms')  # 整理时间
    df = df[['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']].copy()  # 整理列的顺序
    # 去重、排序
    df.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)
    df.sort_values('candle_begin_time', inplace=True)
    df.reset_index(drop=True, inplace=True)

    # 保存数据
    df.to_csv(filepath, index=False)
    print(filepath)


# 多进程清洗数据
@cal_time
def main_clear():
    list_file = get_file_list(filepath, '.csv')
    with ProcessPoolExecutor(max_workers=cpus) as pool:
        pool.map(clear_data, list_file)

    pool.shutdown(True)


# 解压数据
def unzip(filename):
    zip_file = zipfile.ZipFile(filename)
    a_name = zip_file.namelist()
    if (filename.find(".zip")) > -1:
        zip_file = zipfile.ZipFile(filename)
        a_name = zip_file.namelist()
    for names in a_name:
        if (names.find('.csv')) > -1:
            try:
                zip_file.extract(names, filepath)
                print(f"{filename}  解压完成")
            except Exception as e:
                print(e)
                print(f"{filename}  ==========解压失败！========")
                pass

    zip_file.close()
    try:
        os.remove(filename)
        print(f"{filename}  已经删除")
    except FileNotFoundError:
        print(f'{filename}  文件不存在')
    except Exception as _e:
        print(_e)
        print(f'{filename}  ==========删除失败！==========')


# 多进程解压数据
@cal_time
def main_unzip():
    list_file = get_file_list(filepath, 'zip')
    with ProcessPoolExecutor(max_workers=cpus) as pool:
        pool.map(unzip, list_file)

    pool.shutdown(True)


filepath = r'.\test'
cpus = os.cpu_count() * 2
if __name__ == '__main__':
    start = time.time()

    # 下载文件
    main_download()

    # 解压文件
    main_unzip()

    # 清洗数据
    main_clear()

    print(f'总耗时 {time.time() - start}s\n')
