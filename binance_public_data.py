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
import glob
import pandas as pd
from tqdm import tqdm


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
    __columns = {
        0: 'candle_begin_time_ms',
        1: 'open',
        2: 'high',
        3: 'low',
        4: 'close',
        5: 'volume',
        6: 'candle_close_time_ms',
        7: 'quote/base_asset_volume',
        8: 'num_of_trades',
        9: 'Taker_buy_base_asset_volume',
        10: 'Taker_buy_quote_asset_volume',
        11: 'Ignore'
    }

    __list = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote/base_asset_volume', 'num_of_trades', 'Taker_buy_base_asset_volume', 'Taker_buy_quote_asset_volume']



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
    
    def _extract_by_regex(self, string: str, regex: str):
        """
        从string中提取symbol
        """
        match = re.search(regex, string)
        if match:
            result = match.group(1)
            return result

        """
        通过url获取品种
        """
        match = re.search(r"/(futures/cm|spot|futures/im)/", url)
        if match:
            return match.group(1)

    def _generate_urls(self, to_file=False):
        """
        生成url.txt文件，用于下载历史数据
        """
        urls = []
        for type in self._get_type_list():
            zip_urls, checksum_ruls = self._generate_url(type)
            urls += zip_urls
            urls += checksum_ruls
        if to_file:
            with open('url.txt', 'w') as f:
                for url in urls:
                    f.write(url)
                    f.write('\n')
            print("url.txt文件已生成，供参考和检查")
        return urls

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
            
            # 获取symbol
            symbole_regex = r'/(\w+)-\d+m-\d{4}-\d{2}\.zip'
            symbol = self._extract_by_regex(url, symbole_regex)

            # 获取品种
            market_type_regex = r"/(futures/cm|spot|futures/um)/"
            market_type = self._extract_by_regex(url, market_type_regex)

            # 获取路径
            path = os.path.join(self.__data, market_type, symbol, 'zip')

            # 判断文件夹是否存在，不存在则创建
            if not os.path.exists(path):
                os.makedirs(path)
            
            # 保存文件
            new_path = os.path.join(path, url.split("/")[-1])  # 这里要看下保存文件的路径是什么
            with open(new_path, 'wb') as f:
                f.write(res.content)

        except requests.HTTPError as e:
            # print(e)
            # print(f'{url} 文件不存在')
            pass
        except Exception as e:
            # print(e)
            # print(f'{url}  =====下载失败！=====')
            pass
        finally:
            time.sleep(1)

    def download_multiprocess(self):
        """
        多进程下载
        """
        urls = self._generate_urls()
        with ProcessPoolExecutor(max_workers=self.__worker_num) as pool:
            # 显示进度条
            with tqdm(total=len(urls)) as pbar:
                # 提交任务
                for _ in pool.map(self._download, urls):
                    # 更新进度条
                    pbar.update()

        pool.shutdown(True)
    
    def _checksum(self, url):
        """
        验证文件完整性
        """
        # 回到根目录
        os.chdir(self.__root)

        # 获取symbol
        symbole_regex = r'/(\w+)-\d+m-\d{4}-\d{2}\.zip'
        symbol = self._extract_by_regex(url, symbole_regex)

        # 获取品种
        market_type_regex = r"/(futures/cm|spot|futures/um)/"
        market_type = self._extract_by_regex(url, market_type_regex)

        # 设置工作路径
        working_dir = os.path.join(self.__data, market_type, symbol)
        
        # 切换工作路径
        os.chdir(working_dir)

        # 获取路径下的checksum_files
        checksum_files = glob.glob('*.zip.CHECKSUM')

        # 验证文件完整性
        for checksum_file in checksum_files:
            # Run sha256sum command and capture output
            output = subprocess.run(['sha256sum', '-c', checksum_file], capture_output=True, text=True)

            # Check if the output contains the string "OK"
            if 'OK' in output.stdout:
                # print(f"{filename} checksum verified successfully")
                pass
            else:
                # 保存到csv文件
                df = pd.DataFrame({"filename": [checksum_file], "output": [output]})
                file = os.path.join(self.__root, 'checksum.csv')
                df.to_csv(file, mode='a', header=False, index=False)
 
    def checksum_multiprocess(self):
        """
        多进程验证文件完整性
        """
        urls = self._generate_urls()
        with ProcessPoolExecutor(max_workers=self.__worker_num) as pool:
            # 显示进度条
            with tqdm(total=len(urls)) as pbar:
                # 提交任务
                for _ in pool.map(self._checksum, urls):
                    # 更新进度条
                    pbar.update()

        # 保存到csv文件
        file = os.path.join(self.__root, 'checksum.csv')
        try:
            df = pd.read_csv(file, header=None, names=['filename', 'output'])
            df.to_csv(file, index=False)
            print("checksum已检查完毕，请查看checksum.csv文件")
        except FileNotFoundError:
            print("checksum已检查完毕，数据完整")

    def _get_file_relative_path(self, path, file_type: str):
        """
        获取文件相对路径
        """
        relative_path = []
        for root, dirs, files in os.walk(path):  # 当前目录路径，当前路径下所有子目录，当前路径下所有非目录子文件
            for file in files:
                if file.endswith(file_type):  # .zip, .csv
                    file_path = os.path.join(root, file)
                    relative_path.append(os.path.relpath(file_path, self.__root))
        return relative_path

    def _unzip(self, zip_file):
        """
        解压zip文件
        """
        # 设置储存路径
        dir_path = os.path.dirname(zip_file)
        csv_path = dir_path.replace("zip", "csv")
        # print(new_file_path)

        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                csv_file = zip_ref.namelist()[0]
                zip_ref.extract(csv_file, path=csv_path)
                # print(f"{csv_file} extracted successfully")
        except FileExistsError:
            pass

    def unzip_multiprocess(self):
        """
        多进程解压zip文件
        """
        zip_files = self._get_file_relative_path(self.__data, ".zip")
        with ProcessPoolExecutor(max_workers=self.__worker_num) as pool:
            # 显示进度条
            with tqdm(total=len(zip_files)) as pbar:
                # 提交任务
                for _ in pool.map(self._unzip, zip_files):
                    # 更新进度条
                    pbar.update()
        pool.shutdown(True)

    def _drop_dirty_data(self, df):
        """
        检查有没有脏数据在里面
        """
        # Check if the first value in the first column is a timestamp
        try:
            pd.to_datetime(df.iloc[0, 0])
        except ValueError:
            # If it's not a timestamp, delete the first row of the data
            df = df.drop(df.index[0])
        return df

    def _clean_data(self, symbol_path):
        """
        清洗数据
        """
        try:
            # 获取所有csv文件
            csv_path = os.path.join(symbol_path, 'csv')
            csvs = self._get_file_relative_path(csv_path, ".csv")
            # 读取csv文件
            result = pd.DataFrame()
            for csv in csvs:
                df = pd.read_csv(csv, header=None, names=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
                df = self._drop_dirty_data(df)
                result = result.append(df)
            
            # 整理数据
            result.rename(columns=self.__columns, inplace=True)  # 重命名
            result['candle_begin_time'] = pd.to_datetime(result['candle_begin_time_ms'], unit='ms')  # 整理时间
            result = result[self.__list].copy()  # 整理列的顺序
            
            # 去重、排序
            result.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)
            result.sort_values('candle_begin_time', inplace=True)
            result.reset_index(drop=True, inplace=True)

            # 保存数据
            symbol = os.path.basename(symbol_path)
            file = os.path.join(symbol_path, f'{symbol}.csv')
            result.to_csv(file, index=False)
        except KeyError:
            print(f"{symbol} =====清洗失败！=====")
            pass

    def _get_all_symbol_path(self):
        """
        获取所有symbol路径
        """
        # 根据csv文件获取路径
        zip_files = self._get_file_relative_path(self.__data, ".csv")
        pre_symbols = [os.path.dirname(os.path.dirname(zip_file)) for zip_file in zip_files]
        
        # 去除掉重复的路径
        symbols = list(set(pre_symbols))

        # 去除掉不需要的路径
        bad_symbols = [r"data\futures\cm", r"data\futures\um", r"data\spot"]
        symbols = [symbol for symbol in symbols if symbol not in bad_symbols]

        return symbols

    def clean_data_multiprocess(self):
        """
        多进程清洗数据
        """
        # 获取所有zip文件夹的路径
        symbols = self._get_all_symbol_path()

        with ProcessPoolExecutor(max_workers=self.__worker_num) as pool:
            # 显示进度条
            with tqdm(total=len(symbols)) as pbar:
                # 提交任务
                for _ in pool.map(self._clean_data, symbols):
                    # 更新进度条
                    pbar.update()

        pool.shutdown(True)

    def _check_data_integrity(self, symbol_path):
        """
        检查数据完整性
        """
        # 获取csv文件
        regex = r'\\(\w+)$'
        symbol = self._extract_by_regex(symbol_path, regex)
        csv_file = os.path.join(symbol_path, f'{symbol}.csv')
        df = pd.read_csv(csv_file)

        # 检查数据完整性
        start = min(df['candle_begin_time'])
        end = max(df['candle_begin_time'])
        expected = pd.date_range(start=start, end=end, freq='1min').strftime('%Y-%m-%d %H:%M')
        actual = pd.to_datetime(df['candle_begin_time']).dt.strftime('%Y-%m-%d %H:%M')

        missing = expected[~expected.isin(actual)]

        # # 如果有缺失的数据，记录下来
        if len(missing) > 0:
            # 记录缺失的数据
            missing_start = min(missing)
            missing_end = max(missing)
            df = pd.DataFrame({"symbol": [symbol], "missing": [missing], "directory": symbol_path})
            
            # 保存到csv文件
            file = os.path.join(self.__root, 'missing.csv')
            df.to_csv(file, mode='a', header=False, index=False)
        
    def check_data_integrity_multiprocess(self):
        """
        多进程检查数据完整性
        """
        # 获取所有zip文件夹的路径
        symbol_paths = self._get_all_symbol_path()

        with ProcessPoolExecutor(max_workers=self.__worker_num) as pool:
            # 显示进度条
            with tqdm(total=len(symbol_paths)) as pbar:
                # 提交任务
                for _ in pool.map(self._check_data_integrity, symbol_paths):
                    # 更新进度条
                    pbar.update()
            
        # 保存到csv文件
        file = os.path.join(self.__root, 'missing.csv')
        try:
            df = pd.read_csv(file, header=None, names=['symbol', 'missing_start', 'missing_end'])
            df.to_csv(file, index=False)
            print("数据完整性已检查完毕，请查看missing.csv")
        except FileNotFoundError:
            print("数据完整性已检查完毕，数据完整")

    def test(self):
        symbol_path = 'data\spot\ADABTC'
        self._check_data_integrity(symbol_path)
        pass
        
        


    # 交割合约最好是能有提前剪枝的算法，没有也没关系
    # 利用多进程去解压
if __name__ == '__main__':
    print("=====main开始运行=====")
    pbd = BinancePublicData()

    # 测试使用 
    # pbd.test()

    # 多进程下载
    # pbd.download_multiprocess()

    # 多进程检查
    # pbd.checksum_multiprocess()

    # 多进程解压
    # pbd.unzip_multiprocess()  

    # 多进程清洗数据
    # pbd.clean_data_multiprocess()

    # 多进程检查数据完整性
    pbd.check_data_integrity_multiprocess()