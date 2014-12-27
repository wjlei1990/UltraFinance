from StockData import StockOnline
from StockData import StockServer
from StockData import StockClient
import datetime

# pull data from yahoo online service
onlinedata = StockOnline()
stock_list = ('AAPL','IBM')
starttime = datetime.datetime(2008, 1, 1)
endtime = datetime.datetime(2008,1,10)
stock_table = onlinedata.pull_data(stock_list, starttime, endtime)
# print stock_table['AAPL']

# store data into db
tosql = StockServer('root','000539','stock')
tosql.init_db(stock_table,init_mode='replace')
tosql.finalize_engine()

#read data out
fromsql = StockClient('root','000539','stock')
stock_table = fromsql.read_full_stock_record(stock_list)
print stock_table



