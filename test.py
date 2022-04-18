from QUANTAXIS.QASU import main, save_binance

from QUANTAXIS.QAUtil import DATABASE, print_used_time
from QUANTAXIS.QASU.save_binance import QA_SU_save_binance
from QUANTAXIS.QAFetch import Fetcher
from QUANTAXIS.QAUtil.QAParameter import (DATABASE_TABLE, DATASOURCE,
                                          FREQUENCE, MARKET_TYPE,
                                          OUTPUT_FORMAT)
import QUANTAXIS as QA

if __name__ == '__main__':
    #stock list save/read
    # #engine: tushare, tdx,gm,jq
    # main.QA_SU_save_stock_info("tushare", client=DATABASE)
    # print("save done")
    # localFetch = Fetcher.QA_Fetcher()
    # #freq and market type, output format not implemented
    # #tushare don't have multiple/single stocks queries
    # res = localFetch.get_info(["000002","000019"], FREQUENCE.DAY, MARKET_TYPE.STOCK_CN, DATASOURCE.MONGO, OUTPUT_FORMAT.DATAFRAME)
    # print(res)

    # #stock daily save/read, code = '' save all, refresh on last saved to today only
    # main.QA_SU_save_stock_day("tushare", client=DATABASE, paralleled=False, code=["000001.SZ","000002.SZ"])
    # print("save done")
    # #fetch single/list of stock info. try local first, if not found query real time using tdx
    # #daily works fine, weekly will only return one asset
    # res_day = Fetcher.QA_quotation_adv(['000001','000002'], '2019-12-01', '2020-02-03', frequence=FREQUENCE.DAY,
    #                        market=MARKET_TYPE.STOCK_CN, source=DATASOURCE.AUTO, output=OUTPUT_FORMAT.DATAFRAME)
    # print(res_day)

    #crypto save/read
    #save_binance.QA_SU_save_binance_symbol()

    # save_binance.QA_SU_save_binance(frequency='5m', code="ETHBTC")
    #save_binance.QA_SU_save_binance(frequency='5m', code=["LTCBTC"])
    # save_binance.QA_SU_save_binance(frequency='1d', code=["ETHBTC"])
    # test = ["BINANCE.ETHBTC"]
    # DATABASE.cryptocurrency_min.remove({})
    # data1 = Fetcher.QA_quotation_adv(code=test,
    #         start='2021-08-21',
    #         end='2022-04-14',
    #         frequence=FREQUENCE.DAY,
    #         market=MARKET_TYPE.CRYPTOCURRENCY,
    #         output=OUTPUT_FORMAT.DATASTRUCT
    #     )
    # print(data1.data)
    data2 = Fetcher.QA_quotation_adv(
            code=test,
            start='2022-04-12',
            end='2022-04-14 18:10:00',
            frequence='5min',
            market=MARKET_TYPE.CRYPTOCURRENCY,
            output=OUTPUT_FORMAT.DATAFRAME
        )
    print(data2)

    # data_4h = QA.QA_DataStruct_CryptoCurrency_min(data2.resample('4h'))
    # print(data_4h.data)




