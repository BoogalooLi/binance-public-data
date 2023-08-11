"""
利用binance的开源项目，获取历史数据
项目地址：https://github.com/binance/binance-public-data
开源项目提供的网页：https://data.binance.vision
"""
import re
from fake_useragent import UserAgent
import requests
from datetime import datetime, timedelta
import os
import time
from concurrent.futures import ProcessPoolExecutor
import subprocess
import zipfile


# 修饰器
def cal_time(func):
    def _wrapper(*args, **kwargs):
        _start_time = time.time()
        result = func(*args, **kwargs)
        _end_time = time.time()
        print(f'{func.__name__}() 耗时 {round(_end_time - _start_time)}s\n')
        return result

    return _wrapper

class BinancePublicData:
    """
    利用binance的开源项目，获取历史数据
    项目地址：https://github.com/binance/binance-public-data
    开源项目提供的网页：https://data.binance.vision
    """

    # 所有实例的共享变量
    __url_config = {
            "spot": {
                "symbol": "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/spot/monthly/klines/",
                "base": "https://data.binance.vision/data/spot/monthly/klines"
            },
            "cm_future": {
                "symbol": "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/cm/monthly/klines/",
                "base": "https://data.binance.vision/data/futures/cm/monthly/klines"
            },
            "um_future": {
                "symbol": "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/monthly/klines/",
                "base": "https://data.binance.vision/data/futures/um/monthly/klines"
            }
    }
    __start_yyyy_mm = '2017-08'
    __root = os.getcwd()  
    __worker_num = os.cpu_count()
    __lack_data_file = []  # 缺失数据的交易对    

    def __init__(self):
        # 配置文件
        # symbol: 获取历史交易对
        # base: 获取历史数据文件
        self.__intervals = ['1m']
        self.__data = 'data'  # 文件保存路径
        pass
    
    def _generate_yyyy_mm_list(self):
        start_date = datetime.strptime(self.__start_yyyy_mm, '%Y-%m')
        end_date = datetime.now()

        yyyy_mm_list = []
        while start_date <= end_date:
            yyyy_mm_list.append(start_date.strftime('%Y-%m'))
            start_date += timedelta(days=31)
        return yyyy_mm_list

    def _get_type_list(self):
        """
        获取历史数据类型列表
        """
        return list(self.__url_config.keys())
    
    def _get_html_by_type(self, type: str):
        """
        获取网页html
        """
        # 设置爬虫参数
        ua = UserAgent()
        ua_random = ua.random
        headers = {
            'User-Agent': ua_random
        }

        # 获取html
        try:
            res = requests.get(url=self.__url_config[type]["symbol"], headers=headers)
            res.raise_for_status()
            html = res.text
            return html
        except requests.HTTPError as e:
            print(e)
            print(f"====={res} 访问失败=====")
            return None
        except Exception as e:
            print(e)
            print(f"====={res} 访问失败=====")
            return None

    def _get_history_symbol(self, type: str):
        """
        获取历史交易对
        """
        # 定义正则表达式
        regex = r'/klines/(\w+)/</Prefix>'

        # 读取html文件
        html = self._get_html_by_type(type)
        
        # 利用正则表达式匹配
        symbols = re.findall(regex, html)

        return symbols

    def _generate_url(self, type: str):
        """
        生成url, 用于下载历史数据, 包括生成zip文件的url和checksum文件的url
        """
        # 记录urls
        zip_urls = []
        checksum_urls = []
        # 生成标签
        symbols = self._get_history_symbol(type)
        # 生成yyyy-mm列表
        yyyy_mm_list = self._generate_yyyy_mm_list()

        for symbol in symbols:
            for interval in self.__intervals:
                for yyyy_mm in yyyy_mm_list:
                        url = "{}/{}/{}/{symbol}-{interval}-{yyyy_mm}.zip".format(self.__url_config[type]["base"], symbol, interval, symbol=symbol, interval=interval, yyyy_mm=yyyy_mm)
                        zip_urls.append(url)
                        checksum_url = "{}/{}/{}/{symbol}-{interval}-{yyyy_mm}.zip.CHECKSUM".format(self.__url_config[type]["base"], symbol, interval, symbol=symbol, interval=interval, yyyy_mm=yyyy_mm)
                        checksum_urls.append(checksum_url)        
        return zip_urls, checksum_urls
    
    def _extract_symbol(self, string: str, regex: str):
        """
        从string中提取symbol
        """
        match = re.search(regex, string)
        if match:
            result = match.group(1)
            return result

    def _download(self, url):
        """
        下载文件
        """
        # 设置爬虫参数
        ua = UserAgent()
        ua_random = ua.random
        headers = {
            'User-Agent': ua_random
        }

        # 下载文件
        try:
            res = requests.get(url, headers=headers)
            res.raise_for_status()
            
            # 保存文件
            regex = r'/(\w+)-\d+m-\d{4}-\d{2}\.zip'  # 获取symbol
            symbol = self._extract_symbol(url, regex)
            path = os.path.join(self.__data, symbol, 'zip')

            # 判断文件夹是否存在，不存在则创建
            if not os.path.exists(path):
                os.makedirs(path)
            
            # 保存文件
            new_path = os.path.join(path, url.split("/")[-1])  # 这里要看下保存文件的路径是什么
            with open(new_path, 'wb') as f:
                f.write(res.content)

        except requests.HTTPError as e:
            print(e)
            print(f'{url} 文件不存在')
        except Exception as e:
            print(e)
            print(f'{url}  =====下载失败！=====')
        finally:
            time.sleep(1)

    @cal_time
    def download_multiprocess(self):
        """
        多进程下载
        """
        types = self._get_type_list()
        for type in types:
            zip_urls, checksum_urls = self._generate_url(type)
            # combine urls to a list
            urls = zip_urls + checksum_urls
            with ProcessPoolExecutor(max_workers=self.__worker_num) as pool:
                pool.map(self._download, urls)
        pool.shutdown(True)

    def _checksum(self, filename):
        """
        验证文件完整性
        """
        checksum_file = f'{filename}.CHECKSUM'

        # Run sha256sum command and capture output
        output = subprocess.run(['sha256sum', '-c', checksum_file], capture_output=True, text=True)

        # Check if the output contains the string "OK"
        if 'OK' in output.stdout:
            # print(f"{filename} checksum verified successfully")
            pass
        else:
            self.__lack_data_file.append(filename)
            # print(f"{filename} checksum verification failed")

    def checksum_multiprocess(self):
        """
        多进程验证文件完整性
        """
        relative_path = self._get_file_relative_path(".zip")
        with ProcessPoolExecutor(max_workers=self.__worker_num) as pool:
            pool.map(self._checksum, relative_path)
        pool.shutdown(True)

    def _get_file_relative_path(self, file_type: str):
        """
        获取文件相对路径
        """
        relative_path = []
        for root, dirs, files in os.walk(self.__data):
            for file in files:
                if file.endswith(file_type):  # .zip, .csv
                    file_path = os.path.join(root, file)
                    relative_path.append(os.path.relpath(file_path, self.__root))
        return relative_path

    @cal_time
    def _unzip(self, zip_file):
        """
        解压zip文件
        """
        # 获取symbol
        regex = r'{}\\(\w+)\\'.format(self.__data)
        symbol = self._extract_symbol(zip_file, regex)

        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            csv_file = zip_ref.namelist()[0]
            path = os.path.join(self.__data, symbol, 'csv')
            zip_ref.extract(csv_file, path=path)
            print(f"{csv_file} extracted successfully")
    
    def unzip_multiprocess(self):
        """
        多进程解压zip文件
        """
        files = self.get_file_relative_path()
        with ProcessPoolExecutor(max_workers=self.__worker_num) as pool:
            pool.map(self._unzip, files)

        pool.shutdown(True)

    def test(self):
        pass
        
        


    # 交割合约最好是能有提前剪枝的算法，没有也没关系
    # 利用多进程去解压
if __name__ == '__main__':
    print("=====main开始运行=====")
    pbd = BinancePublicData()
    # pbd.test()
    # pbd.download_multiprocess()
