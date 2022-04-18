# coding:utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2021 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
QA fetch module

@yutiansut

QAFetch is Under [QAStandard#0.0.2@10x] Protocol


"""
from QUANTAXIS.QAData.QADataStruct import (QA_DataStruct_Future_day,
                                           QA_DataStruct_Future_min,
                                           QA_DataStruct_Future_realtime,
                                           QA_DataStruct_Stock_day,
                                           QA_DataStruct_Stock_min,
                                           QA_DataStruct_Stock_realtime,
                                           QA_DataStruct_Index_day,
                                           QA_DataStruct_Index_min,
                                           QA_DataStruct_CryptoCurrency_day,
                                           QA_DataStruct_CryptoCurrency_min)
from QUANTAXIS.QAFetch import QAEastMoney as QAEM
from QUANTAXIS.QAFetch import QAQuery
from QUANTAXIS.QAFetch import QAQuery_Advance as QAQueryAdv
from QUANTAXIS.QAFetch import QAQuery_Async as QAQueryAsync
from QUANTAXIS.QAFetch import QATdx as QATdx
from QUANTAXIS.QAFetch import QAThs as QAThs
from QUANTAXIS.QAFetch import QATushare as QATushare
from QUANTAXIS.QAFetch import QAbinance as QABin

from QUANTAXIS.QAUtil.QAParameter import (DATABASE_TABLE, DATASOURCE,
                                          FREQUENCE, MARKET_TYPE,
                                          OUTPUT_FORMAT)
from QUANTAXIS.QAUtil.QASql import QA_util_sql_mongo_setting
from QUANTAXIS.QAUtil.QADate_trade import QA_util_get_next_period
from QUANTAXIS.QAData.data_resample import QA_data_day_resample
from QUANTAXIS.QASU import save_tdx
from QUANTAXIS.QAUtil.QADate import QA_util_time_stamp
import pandas as pd
import datetime


class QA_Fetcher:
    def __init__(self, uri='mongodb://127.0.0.1:27017/quantaxis', username='', password=''):
        """
        初始化的时候 会初始化
        """

        self.database = QA_util_sql_mongo_setting(uri).quantaxis
        self.history = {}
        self.best_ip = QATdx.select_best_ip()
        print("tdx ip done")

    def change_ip(self, uri):
        self.database = QA_util_sql_mongo_setting(uri).quantaxis
        return self

    def get_quotation(self, code=None, start=None, end=None, frequence=None, market=None, source=None, output=None):
        """        
        Arguments:
            code {str/list} -- 证券/股票的代码
            start {str} -- 开始日期
            end {str} -- 结束日期
            frequence {enum} -- 频率 QA.FREQUENCE
            market {enum} -- 市场 QA.MARKET_TYPE
            source {enum} -- 来源 QA.DATASOURCE
            output {enum} -- 输出类型 QA.OUTPUT_FORMAT
        """
        pass

    def get_info(self, code, frequence, market, source, output):
        if source is DATASOURCE.TDX:
            res = QATdx.QA_fetch_get_stock_info(code, self.best_ip)
            return res
        elif source is DATASOURCE.MONGO:
            res = QAQuery.QA_fetch_stock_info(
                code, format=output, collections=self.database.stock_info)
            return res

# todo 🛠 output 参数没有用到， 默认返回的 是 QA_DataStruct


def QA_get_tick(code, start, end, market):
    """
    统一的获取期货/股票tick的接口
    """
    res = None
    if market == MARKET_TYPE.STOCK_CN:
        res = QATdx.QA_fetch_get_stock_transaction(code, start, end)
    elif market == MARKET_TYPE.FUTURE_CN:
        res = QATdx.QA_fetch_get_future_transaction(code, start, end)
    return res


def QA_get_realtime(code, market):
    """
    统一的获取期货/股票实时行情的接口
    """
    res = None
    if market == MARKET_TYPE.STOCK_CN:
        res = QATdx.QA_fetch_get_stock_realtime(code)
    elif market == MARKET_TYPE.FUTURE_CN:
        res = QATdx.QA_fetch_get_future_realtime(code)

    return res


def QA_quotation_adv(code, start, end=save_tdx.now_time(), frequence='1min',
                     market=MARKET_TYPE.STOCK_CN, source=DATASOURCE.AUTO, output=OUTPUT_FORMAT.DATAFRAME):
    """一个统一的获取k线的方法
    如果source=DATASOURCE.AUTO,优先mongo,从本地数据库获取,mongo中未下载的数据从TDX中在线补全。(仅限股票)

    Arguments:
        code {str/list} -- 期货/股票的代码
        start {str} -- 开始日期
        end {str} -- 结束日期
        frequence {enum} -- 频率 QA.FREQUENCE
        market {enum} -- 市场 QA.MARKET_TYPE
        source {enum} -- 来源 QA.DATASOURCE
        output {enum} -- 输出类型 QA.OUTPUT_FORMAT 
    """

    if pd.Timestamp(end) > pd.Timestamp(save_tdx.now_time()):
        end = save_tdx.now_time()
    res = None
    if market == MARKET_TYPE.STOCK_CN:
        if frequence == FREQUENCE.DAY or frequence == FREQUENCE.WEEK:
            if source == DATASOURCE.AUTO:
                try:
                    # 返回的是QA_DataStruct_Stock_day对象，为了与在线获取的数据格式保持统一，转成单索引
                    res = QAQueryAdv.QA_fetch_stock_day_adv(
                        code, start, end).data.reset_index(level='code')
                    # res = QAQueryAdv.QA_fetch_stock_day_adv(
                    #     code, start, end).data.reset_index(level='code')[:14]
                    start_date = res.index[-1]
                    end_date = pd.Timestamp(end)
                    if end_date-start_date > datetime.timedelta(hours=17):
                        # 从TDX补充数据，由于仅考虑个股，在这里不做入库操作，入库还是需要save
                        data_tdx = QATdx.QA_fetch_get_stock_day(
                            code, QA_util_get_next_period(start_date, frequence), end_date, '00')
                        # data_tdx与从数据库获取的数据格式上做一些统一。
                        data_tdx = data_tdx.rename(columns={"vol": "volume"}).drop([
                            'date', 'date_stamp'], axis=1)
                        data_tdx.index = pd.to_datetime(data_tdx.index, utc=False)
                        res = pd.concat([res, data_tdx], sort=True)
                    res = QA_DataStruct_Stock_day(
                        res.reset_index().set_index(['date', 'code']))
                except:
                    res = None
            if source == DATASOURCE.MONGO:
                try:
                    res = QAQueryAdv.QA_fetch_stock_day_adv(code, start, end)
                except:
                    res = None
            if source == DATASOURCE.TDX or res == None:
                res = QATdx.QA_fetch_get_stock_day(code, start, end, '00')
                res = QA_DataStruct_Stock_day(res.set_index(['date', 'code']))
            elif source == DATASOURCE.TUSHARE:
                res = QATushare.QA_fetch_get_stock_day(code, start, end, '00')
            if frequence == FREQUENCE.WEEK:
                res = QA_DataStruct_Stock_day(
                    QA_data_day_resample(res.data))
        elif frequence in [FREQUENCE.ONE_MIN, FREQUENCE.FIVE_MIN, FREQUENCE.FIFTEEN_MIN, FREQUENCE.THIRTY_MIN, FREQUENCE.SIXTY_MIN]:
            if source == DATASOURCE.AUTO:
                try:
                    # 返回的是QA_DataStruct_Stock_day对象，为了与在线获取的数据格式保持统一，转成单索引
                    res = QAQueryAdv.QA_fetch_stock_min_adv(
                        code, start, end, frequence=frequence).data.reset_index(level='code')
                    # res = QAQueryAdv.QA_fetch_stock_min_adv(
                    #     code, start, end, frequence=frequence).data.reset_index(level='code')[:710]
                    start_date = res.index[-1]
                    end_date = pd.Timestamp(end)
                    if end_date > start_date:
                        # 从TDX补充数据，由于仅考虑个股，在这里不做入库操作，入库还是需要save
                        data_tdx = QATdx.QA_fetch_get_stock_min(code, QA_util_get_next_period(
                            start_date, frequence), end_date, frequence=frequence)
                        # data_tdx与从数据库获取的数据格式上做一些统一。
                        data_tdx = data_tdx.rename(columns={"vol": "volume"}).drop(
                            ['date', 'datetime', 'date_stamp', 'time_stamp'], axis=1)
                        data_tdx.index = pd.to_datetime(data_tdx.index, utc=False)
                        res = pd.concat([res, data_tdx], sort=True)
                    res = QA_DataStruct_Stock_day(
                        res.reset_index().set_index(['datetime', 'code']))
                except:
                    res = None
            if source == DATASOURCE.MONGO:
                try:
                    res = QAQueryAdv.QA_fetch_stock_min_adv(
                        code,
                        start,
                        end,
                        frequence=frequence
                    )
                except:
                    res = None
            if source == DATASOURCE.TDX or res == None:
                res = QATdx.QA_fetch_get_stock_min(
                    code,
                    start,
                    end,
                    frequence=frequence
                )
                res = QA_DataStruct_Stock_min(
                    res.set_index(['datetime',
                                   'code'])
                )

    elif market == MARKET_TYPE.FUTURE_CN:
        if frequence == FREQUENCE.DAY:
            if source == DATASOURCE.MONGO:
                try:
                    res = QAQueryAdv.QA_fetch_future_day_adv(code, start, end)
                except:
                    res = None
            if source == DATASOURCE.TDX or res is None:
                res = QATdx.QA_fetch_get_future_day(code, start, end)
                res = QA_DataStruct_Future_day(res.set_index(['date', 'code']))
        elif frequence in [FREQUENCE.ONE_MIN,
                           FREQUENCE.FIVE_MIN,
                           FREQUENCE.FIFTEEN_MIN,
                           FREQUENCE.THIRTY_MIN,
                           FREQUENCE.SIXTY_MIN]:
            if source == DATASOURCE.MONGO:
                try:
                    res = QAQueryAdv.QA_fetch_future_min_adv(
                        code,
                        start,
                        end,
                        frequence=frequence
                    )
                except:
                    res = None
            if source == DATASOURCE.TDX or res is None:
                res = QATdx.QA_fetch_get_future_min(
                    code,
                    start,
                    end,
                    frequence=frequence
                )
                res = QA_DataStruct_Future_min(
                    res.set_index(['datetime',
                                   'code'])
                )

    elif market == MARKET_TYPE.INDEX_CN:
        if frequence == FREQUENCE.DAY:
            if source == DATASOURCE.MONGO:
                try:
                    res = QAQueryAdv.QA_fetch_index_day_adv(code, start, end)
                except:
                    return None
            if source == DATASOURCE.TDX or res == None:
                res = QATdx.QA_fetch_get_index_day(code, start, end)
                res = QA_DataStruct_Index_day(res.set_index(['date', 'code']))
        elif frequence in [FREQUENCE.ONE_MIN,
                           FREQUENCE.FIVE_MIN,
                           FREQUENCE.FIFTEEN_MIN,
                           FREQUENCE.THIRTY_MIN,
                           FREQUENCE.SIXTY_MIN]:
            if source == DATASOURCE.MONGO:
                try:
                    res = QAQueryAdv.QA_fetch_index_min_adv(
                        code,
                        start,
                        end,
                        frequence=frequence
                    )
                except:
                    res = None
            if source == DATASOURCE.TDX or res == None:
                res = QATdx.QA_fetch_get_index_min(
                    code,
                    start,
                    end,
                    frequence=frequence
                )
                res = QA_DataStruct_Index_min(
                    res.set_index(['datetime',
                                   'code'])
                )

    elif market == MARKET_TYPE.OPTION_CN:
        if source == DATASOURCE.MONGO:
            #res = QAQueryAdv.QA_fetch_option_day_adv(code, start, end)
            raise NotImplementedError('CURRENT NOT FINISH THIS METHOD')

    elif market == MARKET_TYPE.CRYPTOCURRENCY:
        if frequence == FREQUENCE.DAY:
            if source == DATASOURCE.AUTO or source == DATASOURCE.MONGO:
                try:
                    # 返回的是QA_DataStruct_Stock_day对象，为了与在线获取的数据格式保持统一，转成单索引
                    res = QAQueryAdv.QA_fetch_cryptocurrency_day_adv(
                        code, start, end).data
                    start_date = res.index.get_level_values('date')[-1]
                    end_date = pd.Timestamp(end)
                    # suppose to fetch data from utc start day to end day = end(local end date) -1
                    # get data from binance if not enough
                    # not yet tested
                    if end_date-start_date > datetime.timedelta(hours=24):
                        if type(code)!=list: code = [code]
                        code = [symbol.split(".")[1] for symbol in code if symbol[:7]=="BINANCE"]
                        data_bin = QABin.QA_fetch_binance_kline(code, QA_util_time_stamp(start_date), QA_util_time_stamp(end_date), frequency = frequence,
                                                                callback_func=None)
                        data_bin = data_bin.rename(columns={"symbol": "code"}).drop(
                            ['date_stamp', 'created_at', 'updated_at'], axis=1)
                        data_bin = data_bin.assign(datetime=pd.to_datetime(data_bin['date'], utc=False)).set_index(
                            ['date', 'code'], drop=True)
                        res = pd.concat([res, data_bin], sort=True)
                        res = res[~res.index.duplicated(keep='first')]
                    res = QA_DataStruct_CryptoCurrency_day(res)
                except:
                    res = None
            #implement binance one
            if source == DATASOURCE.BINANCE:
                res = QABin.QA_fetch_binance_kline(code,start,end,frequence,callback_func=None)
                res = QA_DataStruct_CryptoCurrency_day(res.set_index(['date', 'code']))

        elif frequence in [FREQUENCE.ONE_MIN, FREQUENCE.FIVE_MIN, FREQUENCE.FIFTEEN_MIN, FREQUENCE.THIRTY_MIN, FREQUENCE.SIXTY_MIN]:
            if source == DATASOURCE.AUTO or source == DATASOURCE.MONGO:
                """
                        crypto read from local, if latest in local is more than freq to end date, read additional from binance
                        fetch multiple/single code from local/binance directly. using local start end time to fetch [start, end]
                        filter by timestamp and datestamp in utc

                        issue:
                        read additional from binance: 
                        1. can’t perform resample on the result data struct if concat two frames in addition. 
                        2. start/end for fetching additional data not individual symbol based. only based on last symbol’s latest time. 
                """
                try:
                    # 返回的是QA_DataStruct_Stock_day对象，为了与在线获取的数据格式保持统一，转成单索引
                    res = QAQueryAdv.QA_fetch_cryptocurrency_min_adv(
                        code, start, end, frequence=frequence).data
                    try:
                        start_date = res.index.get_level_values('datetime')[-1]
                    except:
                        start_date = pd.Timestamp(start)
                    end_date = pd.Timestamp(end)
                    if end_date-start_date>datetime.timedelta(minutes = int(frequence[:-3])) and source==DATASOURCE.AUTO:
                        if type(code)!=list: code = [code]
                        code = [symbol.split(".")[1] for symbol in code if symbol[:7]=="BINANCE"]
                        data_bin = QABin.QA_fetch_binance_kline(code, QA_util_time_stamp(start_date), QA_util_time_stamp(end_date), frequency = frequence,
                                                                callback_func=None)
                        # # data_tdx与从数据库获取的数据格式上做一些统一。
                        #resample on this one has issue!!
                        data_bin = data_bin.rename(columns={"symbol": "code"}).drop(['date_stamp','created_at','updated_at'], axis=1)
                        data_bin = data_bin.assign(datetime=pd.to_datetime(data_bin['datetime'], utc=False)).set_index(['datetime', 'code'], drop=True)
                        res = pd.concat([res, data_bin], sort=True)
                        res = res[~res.index.duplicated(keep='first')]
                        print("fetching from binance on missing data from {} to {}".format(data_bin.index.get_level_values('datetime')[0],data_bin.index.get_level_values('datetime')[-1]))
                    res = QA_DataStruct_CryptoCurrency_min(res)
                except:
                    res = None
            # implement binance one not tested yet
            if source == DATASOURCE.BINANCE:
                if type(code) != list: code = [code]
                code = [symbol.split(".")[1] for symbol in code if symbol[:7] == "BINANCE"]
                res = QABin.QA_fetch_binance_kline(code, QA_util_time_stamp(pd.Timestamp(start)), QA_util_time_stamp(pd.Timestamp(end)), frequence, callback_func=None)
                res = QA_DataStruct_CryptoCurrency_min(res.set_index(['date', 'code']))
        else:
            print("unsupported QA frequency!")

    #print(type(res))

    if output is OUTPUT_FORMAT.DATAFRAME:
        return res.data
    elif output is OUTPUT_FORMAT.DATASTRUCT:
        return res
    elif output is OUTPUT_FORMAT.NDARRAY:
        return res.to_numpy()
    elif output is OUTPUT_FORMAT.JSON:
        return res.to_json()
    elif output is OUTPUT_FORMAT.LIST:
        return res.to_list()


def QA_quotation(code, start, end, frequence, market, source=DATASOURCE.TDX, output=OUTPUT_FORMAT.DATAFRAME):
    """一个统一的获取k线的方法
    如果使用mongo,从本地数据库获取,失败则在线获取

    Arguments:
        code {str/list} -- 期货/股票的代码
        start {str} -- 开始日期
        end {str} -- 结束日期
        frequence {enum} -- 频率 QA.FREQUENCE
        market {enum} -- 市场 QA.MARKET_TYPE
        source {enum} -- 来源 QA.DATASOURCE
        output {enum} -- 输出类型 QA.OUTPUT_FORMAT
    """
    res = None
    if market == MARKET_TYPE.STOCK_CN:
        if frequence == FREQUENCE.DAY:
            if source == DATASOURCE.MONGO:
                try:
                    res = QAQueryAdv.QA_fetch_stock_day_adv(code, start, end)
                except:
                    res = None
            if source == DATASOURCE.TDX or res == None:
                res = QATdx.QA_fetch_get_stock_day(code, start, end, '00')
                res = QA_DataStruct_Stock_day(res.set_index(['date', 'code']))
            elif source == DATASOURCE.TUSHARE:
                res = QATushare.QA_fetch_get_stock_day(code, start, end, '00')
        elif frequence in [FREQUENCE.ONE_MIN, FREQUENCE.FIVE_MIN, FREQUENCE.FIFTEEN_MIN, FREQUENCE.THIRTY_MIN, FREQUENCE.SIXTY_MIN]:
            if source == DATASOURCE.MONGO:
                try:
                    res = QAQueryAdv.QA_fetch_stock_min_adv(
                        code, start, end, frequence=frequence)
                except:
                    res = None
            if source == DATASOURCE.TDX or res == None:
                res = QATdx.QA_fetch_get_stock_min(
                    code, start, end, frequence=frequence)
                res = QA_DataStruct_Stock_min(
                    res.set_index(['datetime', 'code']))

    elif market == MARKET_TYPE.FUTURE_CN:
        if frequence == FREQUENCE.DAY:
            if source == DATASOURCE.MONGO:
                try:
                    res = QAQueryAdv.QA_fetch_future_day_adv(code, start, end)
                except:
                    res = None
            if source == DATASOURCE.TDX or res is None:
                res = QATdx.QA_fetch_get_future_day(code, start, end)
                res = QA_DataStruct_Future_day(res.set_index(['date', 'code']))
        elif frequence in [FREQUENCE.ONE_MIN, FREQUENCE.FIVE_MIN, FREQUENCE.FIFTEEN_MIN, FREQUENCE.THIRTY_MIN, FREQUENCE.SIXTY_MIN]:
            if source == DATASOURCE.MONGO:
                try:
                    res = QAQueryAdv.QA_fetch_future_min_adv(
                        code, start, end, frequence=frequence)
                except:
                    res = None
            if source == DATASOURCE.TDX or res is None:
                res = QATdx.QA_fetch_get_future_min(
                    code, start, end, frequence=frequence)
                res = QA_DataStruct_Future_min(
                    res.set_index(['datetime', 'code']))

    elif market == MARKET_TYPE.INDEX_CN:
        if frequence == FREQUENCE.DAY:
            if source == DATASOURCE.MONGO:
                try:
                    res = QAQueryAdv.QA_fetch_index_day_adv(code, start, end)
                except:
                    return None
            if source == DATASOURCE.TDX or res == None:
                res = QATdx.QA_fetch_get_index_day(code, start, end)
                res = QA_DataStruct_Index_day(res.set_index(['date', 'code']))
        elif frequence in [FREQUENCE.ONE_MIN, FREQUENCE.FIVE_MIN, FREQUENCE.FIFTEEN_MIN, FREQUENCE.THIRTY_MIN, FREQUENCE.SIXTY_MIN]:
            if source == DATASOURCE.MONGO:
                try:
                    res = QAQueryAdv.QA_fetch_index_min_adv(
                        code, start, end, frequence=frequence)
                except:
                    res = None
            if source == DATASOURCE.TDX or res == None:
                res = QATdx.QA_fetch_get_index_min(
                    code, start, end, frequence=frequence)
                res = QA_DataStruct_Index_min(
                    res.set_index(['datetime', 'code']))

    elif market == MARKET_TYPE.OPTION_CN:
        if source == DATASOURCE.MONGO:
            #res = QAQueryAdv.QA_fetch_option_day_adv(code, start, end)
            raise NotImplementedError('CURRENT NOT FINISH THIS METHOD')
    # print(type(res))

    if output is OUTPUT_FORMAT.DATAFRAME:
        return res.data
    elif output is OUTPUT_FORMAT.DATASTRUCT:
        return res
    elif output is OUTPUT_FORMAT.NDARRAY:
        return res.to_numpy()
    elif output is OUTPUT_FORMAT.JSON:
        return res.to_json()
    elif output is OUTPUT_FORMAT.LIST:
        return res.to_list()


class AsyncFetcher():
    def __init__(self):
        pass

    async def get_quotation(self, code=None, start=None, end=None, frequence=None, market=MARKET_TYPE.STOCK_CN, source=None, output=None):
        if market is MARKET_TYPE.STOCK_CN:
            if frequence is FREQUENCE.DAY:
                if source is DATASOURCE.MONGO:
                    res = await QAQueryAsync.QA_fetch_stock_day(code, start, end)
                elif source is DATASOURCE.TDX:
                    res = QATdx.QA_fetch_get_stock_day(
                        code, start, end, frequence=frequence)
            elif frequence in [FREQUENCE.ONE_MIN, FREQUENCE.FIVE_MIN, FREQUENCE.FIFTEEN_MIN, FREQUENCE.THIRTY_MIN, FREQUENCE.SIXTY_MIN]:
                if source is DATASOURCE.MONGO:
                    res = await QAQueryAsync.QA_fetch_stock_min(code, start, end, frequence=frequence)
                elif source is DATASOURCE.TDX:
                    res = QATdx.QA_fetch_get_stock_min(
                        code, start, end, frequence=frequence)
        return res


if __name__ == '__main__':
    # import asyncio
    # print(QA_quotation_adv('000001', '2020-01-01', '2020-02-03', frequence=FREQUENCE.DAY,
    #                        market=MARKET_TYPE.STOCK_CN, source=DATASOURCE.AUTO, output=OUTPUT_FORMAT.DATAFRAME))
    # print(QA_quotation_adv('000001', '2020-01-22', '2020-02-03 15:00:00', frequence=FREQUENCE.ONE_MIN,
    #                        market=MARKET_TYPE.STOCK_CN, source=DATASOURCE.AUTO, output=OUTPUT_FORMAT.DATAFRAME))
    print(QA_quotation_adv('000001', '2019-12-01', '2020-02-03', frequence=FREQUENCE.WEEK,
                           market=MARKET_TYPE.STOCK_CN, source=DATASOURCE.AUTO, output=OUTPUT_FORMAT.DATAFRAME))
    # Fetcher = AsyncFetcher()
    # loop = asyncio.get_event_loop()
    # res = loop.run_until_complete(asyncio.gather(
    #     # 这几个是异步的
    #     Fetcher.get_quotation('000001', '2018-07-01', '2018-07-15',
    #                           FREQUENCE.DAY, MARKET_TYPE.STOCK_CN, DATASOURCE.MONGO),
    #     Fetcher.get_quotation('000001', '2018-07-12', '2018-07-15',
    #                           FREQUENCE.FIFTEEN_MIN, MARKET_TYPE.STOCK_CN, DATASOURCE.MONGO),
    #     # 这个是同步的
    #     Fetcher.get_quotation('000001', '2018-07-12', '2018-07-15',
    #                           FREQUENCE.FIFTEEN_MIN, MARKET_TYPE.STOCK_CN, DATASOURCE.TDX),
    # ))

    # print(res)
