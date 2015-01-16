from sp500 import sp500
"""
Option Robot
"""

# get sp500 list
get_sp500 = sp500()
get_sp500.pull_data()
print len(get_sp500.stock_info_table)
ticket_list = get_sp500.get_sp500_ticket_list()
print ticket_list

# update ticket_list in database
from TicketListUtil import TicketListSQLUtil
ticketsql = TicketListSQLUtil('apc524', 'apc524', 'optionprice', host='junyic.net')
ticketsql.update_sql(ticket_list)
ticketsql.finalize()

# read in the updated ticket list in db
ticketsql.read_from_sql()
ticket_list = ticketsql.ticket_table
print "Total number of stocks: %d" % len(ticket_list)
print ticket_list
ticketsql.finalize()

from option import OptionScraper, OptionServer

#ticket_list=['AAPL', 'IBM']
optionsql=OptionServer('apc524', 'apc524', 'optionprice', host='junyic.net')
optionsql.update_db(ticket_list)
print optionsql.get_largest_quote_time('AAPL')