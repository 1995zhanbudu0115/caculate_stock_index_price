# coding: utf-8
# Project: 给定股票和权重计算组合(指数)的收盘价
# Author: Alvin Du

from __future__ import division
import datetime
import time
from WindPy import w
import pandas as pd
import numpy as np
import os

w.start()
logfile = open('log.txt', 'w')


def get_codes(file_name):
    out_name = "./SimpleStockData/"
    df = pd.read_csv(out_name + file_name, index_col=0, encoding='gb18030')
    return df['stock'].tolist()


def get_weights(file_name):
    out_name = "./SimpleStockData/"
    df = pd.read_csv(out_name + file_name, index_col=0, encoding='gb18030')
    return df['0'].tolist()


def get_mdays(start, end, fn):
    if os.path.exists(fn):
        df = pd.read_csv("./" +fn, index_col=0)
    else:
        mdays = w.tdays(start, end, "Period=M")
        df = pd.DataFrame({})
        df['mdays'] = list(map(lambda x: str(x)[0:10], mdays.Data[0]))
        df.to_csv("./"+fn)
    return df['mdays'].tolist()


def get_price(all, start, end):
    wdata = pd.DataFrame({})
    start_time = start + " 15:00"
    end_time = end + " 15:00"
    for code in all.columns:
        wdata[code] = all.loc[(start_time <= all.index) & (all.index <= end_time), code]
    return wdata


def get_simple_stock_data(codes, start, end, field, store):
    # dir_name = "X:/StockData/"
    dir_name = "W:/StockData/"
    # dir_name = "Z:/Personal/StockData/"
    out_name = "./SimpleStockData/"
    wdata = pd.DataFrame({})
    cnt = 0
    if os.path.exists(out_name + store):
        return pd.read_csv(out_name + store, index_col=0)
    else:
        for code in codes:
            cnt += 1
            if os.path.exists(out_name + code + ".csv"):
                df0 = pd.read_csv(out_name + code + ".csv", index_col=0)
                wdata[code] = df0.loc[(start <= df0['tradingday']) & (df0['tradingday'] <= end), field]
            else:
                try:
                    df0 = pd.read_csv(dir_name + code + ".csv", index_col=0)
                    df1 = df0[[field, 'tradingday']]
                    df1.to_csv(out_name + code + ".csv")
                    wdata[code] = df1.loc[(start <= df1['tradingday']) & (df1['tradingday'] <= end), field]
                except:
                    wdata[code] = np.nan
                    logfile.write(code + "\n")
            print(str(cnt) + " finished, " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        wdata.to_csv(out_name + store)
        return wdata


def extract_min(doc, start_day, end_day):
    # start_day = "2018-06-29"
    # start_day = "2016-01-31"
    # end_day = "2018-07-26"
    # mdays = get_mdays(start_day, end_day, "mdays" + start_day+"_"+end_day+".csv")
    mdays = [start_day, end_day]
    datelist = pd.DataFrame({'date': mdays}, index=mdays)

    outlist = pd.DataFrame({})
    for n in range(len(datelist)-1):
        date = datelist.date[n]
        date_nextmonth = datelist.date[n + 1]

        codes = get_codes(str(doc)+'.csv')
        weight = get_weights(str(doc)+'.csv')
        all = get_simple_stock_data(codes, date.replace('-', '/'), date_nextmonth.replace('-', '/'), 'adj_close', str(doc) + '_' + date.replace('-', '')+date_nextmonth.replace('-', '')+'.csv')

        wdata = get_price(all, date.replace('-', '/'), date_nextmonth.replace('-', '/'))
        timelist = pd.DataFrame({}, index=wdata.index[1:])

        wpct = np.log(wdata/wdata.shift(1))
        data = wpct.multiply(weight)

        timelist['index'] = data.sum(axis=1)
        outlist = outlist.append(timelist)
        print(date + "," +str(n) + " finished, " + str(doc) + ", " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        doc += 1
    return outlist


if __name__ == "__main__":
    # outlist = extract_min("2018-06-29", "2018-07-31")
    start_day = "2018-09-13"
    end_day = "2018-09-14"
    outlist = extract_min(163, start_day, end_day) # 162表示期
    outlist.to_csv('./MH02_150_' + end_day.replace("-", "") + '.csv')
    print("OK")