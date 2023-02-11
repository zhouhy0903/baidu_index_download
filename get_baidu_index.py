import pandas as pd
import time
from qdata.baidu_index import get_feed_index, get_news_index, get_search_index
from typing import List, Dict
import os
from tqdm import tqdm

SAVEPATH = None
INDEX_FEED_SAVEPATH = None
INDEX_NEWS_SAVEPATH = None
INDEX_SEARCH_SAVEPATH = None
COOKIES = None
INTERVAL_SLEEP = None
START_DATE = None
END_DATE = None


def init(savepath, cookies, start_date, end_date, interval_sleep):
    global SAVEPATH, INDEX_FEED_SAVEPATH, INDEX_NEWS_SAVEPATH, INDEX_SEARCH_SAVEPATH, COOKIES
    global START_DATE, END_DATE, INTERVAL_SLEEP
    SAVEPATH = savepath
    INDEX_FEED_SAVEPATH = os.path.join(savepath, 'feed')
    INDEX_NEWS_SAVEPATH = os.path.join(savepath, 'news')
    INDEX_SEARCH_SAVEPATH = os.path.join(savepath, 'search')
    COOKIES = cookies
    START_DATE = start_date
    END_DATE = end_date
    INTERVAL_SLEEP = interval_sleep

def sleep(t):
    print(f'sleep {t} s...')
    time.sleep(t)

class bData():
    @staticmethod
    def transform_result(x: List[Dict]) -> pd.DataFrame():
        df = pd.DataFrame(x)
        df['date'] = pd.to_datetime(df['date'])
        df['index'] = df['index'].astype(int)
        return df
    
    @staticmethod
    def get_stock_feed_index(keywords: List[List[str]],
                             start_date: str,
                             end_date: str,
                             cookies: str):
        """资讯指数"""
        return get_feed_index(keywords_list = keywords,
                             start_date = start_date,
                             end_date = end_date,
                             cookies = cookies)

    @staticmethod
    def get_stock_news_index(keywords: List[List[str]],
                             start_date: str,
                             end_date: str,
                             cookies: str):
        """媒体指数"""
        return get_news_index(keywords_list = keywords,
                             start_date = start_date,
                             end_date = end_date,
                             cookies = cookies)
        

    @staticmethod
    def get_stock_search_index(keywords: List[List[str]],
                             start_date: str,
                             end_date: str,
                             cookies: str):
        """搜索指数"""
        return get_search_index(keywords_list = keywords,
                             start_date = start_date,
                             end_date = end_date,
                             cookies = cookies)

    @staticmethod    
    def get_stock_index(stock_code: str, 
                        stock_name: str,
                        cookies: str,
                        start_date: str,
                        end_date: str):
        """stock_name: """
        assert isinstance(stock_name, str)
        stock_name = [[stock_name]]
        # start_date = '2018-01-01'
        # end_date = '2023-01-03'
        feed_index = pd.DataFrame(list(bData.get_stock_feed_index(stock_name, start_date, end_date, cookies)))
        sleep(INTERVAL_SLEEP)
        news_index = pd.DataFrame(list(bData.get_stock_news_index(stock_name, start_date, end_date, cookies)))
        sleep(INTERVAL_SLEEP)
        search_index = pd.DataFrame(list(bData.get_stock_search_index(stock_name, start_date, end_date, cookies)))
        sleep(INTERVAL_SLEEP)


        os.makedirs(INDEX_FEED_SAVEPATH, exist_ok = True)
        os.makedirs(INDEX_NEWS_SAVEPATH, exist_ok = True)
        os.makedirs(INDEX_SEARCH_SAVEPATH, exist_ok = True)

        feed_index.to_csv(os.path.join(INDEX_FEED_SAVEPATH, stock_code + ".csv"), index = False)
        news_index.to_csv(os.path.join(INDEX_NEWS_SAVEPATH, stock_code + ".csv"), index = False)
        search_index.to_csv(os.path.join(INDEX_SEARCH_SAVEPATH, stock_code + ".csv"), index = False)

    @staticmethod
    def get_stocks_index(stock_names: Dict[str, str], cookies: str, start_date: str, end_date: str):
        for stock_code, stock_name in tqdm(stock_names.items()):
            try:
                print(stock_code, stock_name)
                bData.get_stock_index(stock_code = stock_code, 
                                      stock_name = stock_name,
                                      cookies = cookies,
                                      start_date = start_date,
                                      end_date = end_date)
                with open("success.txt", 'a+') as f:
                    f.write(stock_code + "\n")
            except:
                with open('log.txt', 'a+') as f:
                    f.write(stock_code + " failed!\n")
                sleep(INTERVAL_SLEEP)

def get_baidu_index():
    stock_names = pd.read_csv("symbol_name.csv")
    def remove_alpha(x):
        new_x = ""
        for i in range(len(x)):
            if not ("A" <= x[i] <= "Z" or "a" <= x[i] <= "z"):
                new_x = new_x + x[i]
        return new_x
    stock_names['symbol'] = stock_names['symbol'].apply(lambda x: x[:6])
    stock_names['name'] = stock_names['name'].apply(lambda x: remove_alpha(x))
    zz500 = pd.read_csv('zz500_stocks.csv', encoding = 'gbk')[['code', 'code_name']]
    zz500['code'] = zz500['code'].apply(lambda x: x[3:])
    zz500.columns = ['symbol', 'code_name']
    stock_names = pd.merge(stock_names, zz500, on = 'symbol', how = 'right')[['symbol', 'name']]
    print(stock_names.head())
    stock_names = stock_names[:1]
    sn = dict(stock_names.values)
    print(sn, COOKIES, START_DATE, END_DATE)
    bData.get_stocks_index(sn, COOKIES, START_DATE, END_DATE)

if __name__ == '__main__':
    init(savepath = './data', 
         # baidu account cookies
         cookies = '', 
         start_date = '2017-01-01',
         end_date = '2022-12-31',
         # sleep 20min 
         interval_sleep = 1200)
    get_baidu_index()

