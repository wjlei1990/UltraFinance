from StockData import StockOnline
from StockData import StockServer
from StockData import StockClient
import datetime
from sp500 import sp500

get_sp500=sp500()
stock_list_all=get_sp500.method()
print len(stock_list_all)
# print stock_list


# pull data from yahoo online service
onlinedata = StockOnline()
#stock_list = ('IBM','AAPL')
starttime = datetime.datetime(1990, 1, 1)
endtime = StockServer.get_cur_date()

print starttime, endtime


# print stock_table['AAPL']

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



#read data out
#fromsql = StockClient('root','000539','stock')
#stock_table = fromsql.read_full_stock_record(stock_list)
#print stock_table



