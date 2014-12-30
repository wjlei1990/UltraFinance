from StockData import StockOnline
from StockData import StockServer
from StockData import StockClient
import datetime
from sp500 import sp500
from sqlalchemy_stubs import *

"""
get_sp500=sp500()
stock_list_all=get_sp500.method()
print len(stock_list_all)
# print stock_list


# pull data from yahoo online service
onlinedata = StockOnline()
stock_list = ('IBM','AAPL')
starttime = datetime.datetime(1990, 1, 1)
endtime = StockServer.get_cur_date()

print starttime, endtime

   
# print stock_table['AAPL']
"""

"""
# store data into db
tosql = StockServer('apc524','apc524','stockprice',host='junyic.net')

for stock_name in stock_list_all:
    stock_list = [stock_name, ]
    stock_table = onlinedata.pull_data(stock_list, starttime, endtime)
    tosql.init_db(stock_table, init_mode='replace')

tosql.finalize_engine()
#print tosql.get_largest_date('IBM')
#print tosql.get_largest_date('AAPL')

#tosql.update_db(stock_list)
"""

stock_list = ('IBM','AAPL')
onlinedata = StockOnline()
starttime = datetime.datetime(2014, 1, 1)
endtime = datetime.datetime(2014,1,10)
stock_table = onlinedata.pull_data(stock_list, starttime, endtime)



starttime = datetime.datetime(2014, 12, 1)
endtime = datetime.datetime(2014, 12, 20)

#read data out
fromsql = StockClient('apc524','apc524','stockprice',host='junyic.net')
#fromsql = StockClient('root','000539','stock')
df = fromsql.read_stock_record(stock_list, starttime, endtime)
#print sk_table
print df['AAPL']
print df['IBM']


