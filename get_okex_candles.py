from urllib.request import urlopen, Request
import requests
import os
import json
import pandas as pd
import datetime
from time import sleep

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行

def send_dingding_msg(content1, robot_id='5a85289ad222d5dd325aaa3f312a73102b23400f0b16c7a5e0e6bbb703f62b3f'):
    try:
        msg = {
            "msgtype": "text",
            "text": {"content": content1 + '\n' + datetime.datetime.now().strftime("%m-%d %H:%M:%S")}
        }
        Headers = {"Content-Type": "application/json;charset=utf-8;"}
        url1 = 'https://oapi.dingtalk.com/robot/send?access_token=' + robot_id
        body = json.dumps(msg)
        requests.post(url1, data=body, headers=Headers)
    except Exception as err:
        print('钉钉发送失败', err)
 

# ===抓取数据，带有浏览器伪装
def get_url_content2(url, max_try_number=5,content1='Boss，交易所行情数据故障，请检查一下！！！'):
    try_num = 0
    while True:
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
            request = Request(url=url, headers=headers)
            content = urlopen(request, timeout=15).read()
            return content
        except Exception as http_err:
            print(url, "抓取报错", http_err)
            try_num += 1
            if try_num >= max_try_number:
                print("尝试失败次数过多，放弃尝试")
                send_dingding_msg(content1)
                return None

# 获取candle数据
def get_candle_from_okex(symbol_list=['btc_usdt'], kline_type='1min'):

    # 创建一个空的df
    df = pd.DataFrame()

    # 遍历每一个symbol
    for symbol in symbol_list:
        sleep(1)
        print(symbol)
        # # 构建url
        url = 'https://www.okex.com/api/v1/kline.do?symbol=%s&type=%s' % (symbol, kline_type)
        print(url)
        # # 抓取数据
        content = get_url_content2(url)
        if content is None:  # 当返回内容为空的时候，跳过本次循环
            return pd.DataFrame()

        # 将数据转化为dataframe
        json_data = json.loads(content.decode("utf-8"))
        df = pd.DataFrame(json_data, dtype='float')

        # # 整理dataframe
        df['symbol'] = symbol
        df.rename(columns={0: 'candle_begin_time', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'volume'}, inplace=True)
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms')
        df['candle_begin_time_GMT8'] = df['candle_begin_time'] + pd.Timedelta(hours=8)
        df.to_csv('/Users/apmcoin/Desktop/get_okex_Kline.csv',index=0)
        return df

while True:
    get_candle_from_okex(symbol_list=['btc_usdt', 'ltc_usdt', 'eth_usdt','eos_usdt','etc_usdt','xrp_usdt','bch_usdt'], kline_type='1min')
    sleep(60)
