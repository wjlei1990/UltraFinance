from sp500 import sp500

# get sp500 list
get_sp500 = sp500()
get_sp500.pull_data()
print len(get_sp500.stock_info_table)
ticket_list = get_sp500.get_sp500_ticket_list()
print ticket_list

root_user     = 'apc524'
root_passwd   = 'apc524'
database_name = 'stockprice'
hostname      = 'junyic.net'

# update ticket_list in database
from TicketListUtil import TicketListSQLUtil
ticketsql = TicketListSQLUtil(root_user, root_passwd, database_name, host=hostname)
ticketsql.update_sql(ticket_list)

# read in the updated ticket list in db
ticketsql.read_from_sql()
ticket_list = ticketsql.ticket_table
print "Total number of stocks: %d" % len(ticket_list)
print ticket_list
ticketsql.finalize()

#ticket_list = ['AAPL', 'IBM']
from StockData import StockServer
tosql = StockServer(root_user, root_passwd, database_name, host=hostname)
tosql.update_db(ticket_list)
tosql.update_index_table()
tosql.finalize()
