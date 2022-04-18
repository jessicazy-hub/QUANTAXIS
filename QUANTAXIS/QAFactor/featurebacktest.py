#
from uuid import uuid4

import pandas as pd

from QUANTAXIS.QAUtil import QA_util_get_last_day, QA_util_get_trade_range, QA_util_code_change_format
from QUANTAXIS.QAFactor.featureView import QAFeatureView
from QUANTAXIS.QIFI.QifiAccount import QIFI_Account
from QUANTAXIS.QAFetch.QAClickhouse import QACKClient
from QUANTAXIS.QAFetch import Fetcher
from dateutil import parser
from qaenv import clickhouse_ip, clickhouse_password, clickhouse_user, clickhouse_port, mongo_ip
from QUANTAXIS.QAUtil.QAParameter import (DATABASE_TABLE, DATASOURCE,
                                          FREQUENCE, MARKET_TYPE,
                                          OUTPUT_FORMAT)
from QUANTAXIS.QAIndicator.indicators import QA_indicator_MA
import datetime
"""
backtest for feature data

"""


class QAFeatureBacktest():
    def __init__(self, feature, quantile=0.996, init_cash=50000000, frequence = "5min", rolling=5,portfolioname='feature', mongo_ip =mongo_ip,
                 clickhouse_host=clickhouse_ip, clickhouse_port=clickhouse_port, clickhouse_user=clickhouse_user, clickhouse_password=clickhouse_password) -> None:
        """
        feature --> standard QAFeature
        quantile -> prue long only   upper quantile can be selected

        init_cash --> account backtest initcash

        rolling --> dategap for rolling sell

        clickhouse should be save data first
        
        mongoip -->  use to save qifiaccount

        """
        self.dateOrMin = "datetime" if frequence[-3:]=="min" else "date"
        self.feature = feature.reset_index(drop=True).drop_duplicates(
            [self.dateOrMin, 'code']).set_index([self.dateOrMin, 'code'], drop=True).sort_index().dropna()
        self.featurename = self.feature.columns[0]
        self.start = str(self.feature.index.levels[0][0]) if self.dateOrMin == "datetime" else str(self.feature.index.levels[0][0])[0:10]
        self.end = str(self.feature.index.levels[0][-1]) if self.dateOrMin == "datetime" else str(self.feature.index.levels[0][-1])[0:10]
        self.codelist = self.feature.index.levels[1].tolist()

        self.client = QACKClient(
            host=clickhouse_host, port=clickhouse_port, user=clickhouse_user, password=clickhouse_password)
        self.quantile = quantile
        self.preload = self.feature.groupby(
            level=1, as_index=False, group_keys=False).apply(lambda x: self.slice_feature(x))
        # self.datacenter = self.client.get_stock_day_qfq_adv(
        #     self.codelist, self.start, self.end)
        # self.closepanel = self.datacenter.closepanel.bfill() ## 向前复权 匹配股票停牌模式 使用复牌后第一个收盘价卖出
        self.account = QIFI_Account(init_cash=init_cash, username='QAFB_{}_{}'.format(self.featurename, uuid4()), broker_name='feature', portfolioname=portfolioname,
                                    password='1', nodatabase=False, model='BACKTEST', trade_host=mongo_ip)
        self.tradetable = {}
        self.rolling = rolling
        self.cashpre = init_cash/rolling
        self.frequence = frequence
        self.account.initial()
        self.data = Fetcher.QA_quotation_adv(
        code=self.codelist,
        start=self.start,
        end=self.end,
        frequence= self.frequence,
        market=MARKET_TYPE.CRYPTOCURRENCY,
        output=OUTPUT_FORMAT.DATAFRAME
    )


    def slice_feature(self, data):
        res = data[data > data.quantile(self.quantile)].dropna()
        res.index = res.index.remove_unused_levels()
        return res

    def get_feature(self, start, end=None):
        start = start if self.dateOrMin == "datetime" else parser.parse(start).date()

        end = self.end if end is None else self.end if self.dateOrMin == "datetime" else parser.parse(self.end).date()

        return self.feature.loc[start:end, :, :]

    def get_buy_list(self, time):
        """
        date --> real date
        """
        signaltime = time-datetime.timedelta(minutes = int(self.frequence[:-3])) if self.dateOrMin == "datetime" else parser.parse(QA_util_get_last_day(time)).date()
        try:
            buy = self.preload.loc[signaltime, :, :]
            # buy.index = buy.index.remove_unused_levels()
            return buy.index.tolist()
        except:
            return []

    def get_sell_list(self, time):
        #sell # rollings later from the previous buy
        try:
            sell = list(self.tradetable[time-datetime.timedelta(minutes = int(self.frequence[:-3])*(self.rolling-1))].keys()) if self.dateOrMin == "datetime" else list(self.tradetable[QA_util_get_last_day(
                time, self.rolling-1)].keys())
            return sell
        except:
            return []

    def run(self,):
        """
        buy nexttime open
        
        sell next Ntime close

        QA_position(hold details, volume, position, returns current pos and close avaliable)

        order mgmt process
        account:
            trade related: positions, trades, orders, event, frozen
            pnl related: balance, money, available
        account.send_order
            -> account.on_price_change: update datetime & pos last price
            -> order check: get pos if no create new QAPosition with code, determine frozen based on towards
            -> current order creation 下单成功
        account.make_deal
            -> acc.receive_deals: if trade amount = vl, money+=frozen? clear vl 全部成交（默认）
            -> update pos (gives margin, commission and profit), update money = holdings+commission and close_profit = profit-commission
        account.settle
            -> sync db.history with message
            -> reinitialize variables add closeprofit to static_balance.
        """
        timelist = self.feature.index.levels[0] if self.dateOrMin == "datetime" else QA_util_get_trade_range(self.start, self.end)
        for time in timelist:
            buylist = self.get_buy_list(time)
            selllist = self.get_sell_list(time)
            self.tradetable[time] = {}
            if len(buylist) != 0:

                # d = self.datacenter.selects(
                #     buylist, time, time).open.map(lambda x: round(x, 2))
                #
                # d.index = d.index.droplevel(0)
                #
                # data = d.to_dict()

                #get open for the current time
                data = self.data.loc[(time,buylist),'open'].reset_index().drop([self.dateOrMin], axis=1).set_index("code",drop=True)
                data = data["open"]
                cashpre = self.cashpre/len(buylist)
                for code in buylist:
                    try:
                        volume = int(
                            0.01*cashpre/data.loc[code])*100 if data.loc[code] != 0 else 0
                        if volume < 100:
                            pass
                        else:
                            print("\n buy {} with volume {} price {} time {}".format(code, volume, data.loc[code], time))
                            order = self.account.send_order(
                                code, volume, price=data.loc[code], datetime=time, towards=1)
                            self.account.make_deal(order)
                            self.tradetable[time][code] = volume
                    except Exception as e:
                        """
                        主要是停牌买不入 直接放弃

                        此处买入未加入连续一字板的检测 rust 会增加此处的逻辑

                        """
                        pass
            else:
                pass

            if len(selllist) != 0:
                # data = self.closepanel.loc[parser.parse(time).date(), selllist].map(lambda x: round(x,2)).to_dict()
                data = self.data.loc[(time, selllist), 'close'].reset_index().drop([self.dateOrMin], axis=1).set_index("code",
                                                                                                                 drop=True)
                data = data["close"]
                for code in selllist:
                    volume = self.tradetable[time - datetime.timedelta(minutes=int(self.frequence[:-3])* (self.rolling-1))][code] if self.dateOrMin == "datetime" else self.tradetable[QA_util_get_last_day(
                    time, self.rolling-1)][code]
                    if volume < 100:
                        pass
                    else:
                        print("\n sell {} with volume {} price {} time {}".format(code,volume,data.loc[code],time))
                        order = self.account.send_order(
                            code, volume, price=data.loc[code], datetime=time, towards=-1)
                        self.account.make_deal(order)



            # holdinglist = [QA_util_code_change_format(code) for code in list(self.account.positions.keys())]
            # pricepanel = self.closepanel.loc[parser.parse(time).date(), holdinglist].map(lambda x: round(x,2))
            # #pricepanel.index = pricepanel.index.droplevel(0)
            # pricepanel =pricepanel.to_dict()
            # refresh close price and datetime in account for every time step
            holdinglist = list(self.account.positions.keys())
            pricepanel = self.data.loc[(time, holdinglist), 'close'].reset_index().drop([self.dateOrMin], axis=1).set_index("code",
                                                                                                        drop=True)
            pricepanel = pricepanel["close"]
            for code in holdinglist:
                self.account.on_price_change(code, pricepanel[code], datetime = time)
            self.account.settle()
        print("########## SUCCESS #########")
        print("result balance {} initial cash {}".format(self.account.balance, self.account.init_cash))

from QUANTAXIS.QASU import save_binance
if __name__ == "__main__":
    # # crypto save/read
    # save binance symbol list
    save_binance.QA_SU_save_binance_symbol()
    ### save single symbol
    # save_binance.QA_SU_save_binance(frequency=FREQUENCE.FIVE_MIN, code="ETHBTC")
    save_binance.QA_SU_save_binance(frequency=FREQUENCE.FIVE_MIN, code=["ETHBTC","LTCBTC"])


    # ###################################
    # factor creation
    # #click house implementation
    # from QUANTAXIS.QAFactor import QAFeatureView
    # featurepreview = QAFeatureView()
    # # feature = featurepreview.get_single_factor('Factor')
    code = ["BINANCE.ETHBTC","BINANCE.LTCBTC"]
    data = Fetcher.QA_quotation_adv(
        code=code,
        start='2022-04-10',
        end='2022-04-14 18:10:00',
        frequence=FREQUENCE.FIVE_MIN,
        market=MARKET_TYPE.CRYPTOCURRENCY,
        output=OUTPUT_FORMAT.DATASTRUCT
    )
    #print(data.data)
    feature = data.add_func(QA_indicator_MA, 6)
    feature.columns = ['MA30min']
    feature = feature.dropna().reset_index()
    #print(feature)

    QAFB = QAFeatureBacktest(feature, frequence = FREQUENCE.FIVE_MIN)
    #print("test feature",QAFB.get_feature(pd.Timestamp("2022-04-10 00:35:00")))
    QAFB.run()
