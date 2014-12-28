from StockData import StockOnline
from StockData import StockServer
from StockData import StockClient
import datetime

# pull data from yahoo online service
onlinedata = StockOnline()
stock_list = ('IBM','AAPL')
starttime = datetime.datetime(2014, 12, 1)
endtime = datetime.datetime(2014,12,20)
stock_table = onlinedata.pull_data(stock_list, starttime, endtime)
# print stock_table['AAPL']

# store data into db
tosql = StockServer('root','000539','stock')
#tosql.init_db(stock_table,init_mode='replace')
#tosql.finalize_engine()
#print tosql.get_largest_date('IBM')
#print tosql.get_largest_date('AAPL')

tosql.update_db(stock_list)



#read data out
#fromsql = StockClient('root','000539','stock')
#stock_table = fromsql.read_full_stock_record(stock_list)
#print stock_table



