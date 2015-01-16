import sqlalchemy
#engine = sqlalchemy.create_engine('mysql://root:000539@localhost:3306/test')

from option import OptionScraper, OptionServer

ticket_list=['AAPL', 'IBM']
#get_option = OptionScraper()
#option_data = get_option.pull_data(ticket_list)

#df = option_data['AAPL']
#print df['Quote_Time'][0]

optionsql=OptionServer('root', '000539', 'optionprice')
optionsql.update_db(ticket_list)
print optionsql.get_largest_quote_time('AAPL')