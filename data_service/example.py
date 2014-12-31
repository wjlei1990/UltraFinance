#!/usr/bin/env python
# -*- coding: utf-8 -*-

from StockData import StockOnline
from StockData import StockServer
from StockData import StockClient, get_cur_date
import datetime
from sp500 import sp500
from sqlalchemy_stubs import *


get_sp500=sp500()
get_sp500.pull_data()
print len(get_sp500.stock_info_table)
# print stock_list


# pull data from yahoo online service
onlinedata = StockOnline()
stock_list = ('IBM','AAPL')
starttime = datetime.datetime(2014, 12, 1)
endtime = datetime.datetime(2014, 12, 10)

print starttime, endtime
   
# print stock_table['AAPL']



# store data into db
tosql = StockServer('root', '000539', 'test')

for stock_name in stock_list:
    stock_table = onlinedata.pull_data(stock_name, starttime, endtime)
    tosql.init_db(stock_table, init_mode='replace')

tosql.init_index_table()

"""
#tosql.finalize_engine()
#print tosql.get_largest_date('IBM')
#print tosql.get_largest_date('AAPL')

tosql.update_db(stock_list)
"""


"""


stock_list = ('IBM','AAPL')
onlinedata = StockOnline()
starttime = datetime.datetime(2014, 1, 1)
endtime = datetime.datetime(2014,1,10)
stock_table = onlinedata.pull_data(stock_list, starttime, endtime)

"""

"""
starttime = datetime.datetime(2014, 12, 1)
endtime = datetime.datetime(2014, 12, 20)

#read data out
fromsql = StockClient('root', '000539', 'test')
#fromsql = StockClient('root','000539','stock')
df = fromsql.read_stock_record(stock_list, starttime, endtime)

dict1 = fromsql.read_recent_stock_record(stock_list, ndays=20)
#print sk_table
print "HHAA"
print dict1['AAPL']
#print dict2['AAPL']

fromsql.finalize_connection()
"""
