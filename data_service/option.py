from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.types import DATETIME
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, MetaData
import sqlalchemy
import datetime

from pandas.io.data import Options

from SQL_Util import SQLUtil

Base = declarative_base()


class OptionScraper(object):
    """
    Class Server includes methods that pull data from online resource and store them into database
    """
    def __init__(self, source='yahoo'):
        if source not in ['yahoo', 'google', 'fred']:
            raise ValueError('Current Source only supports: yahoo, google and fred')
        self.source = source
        if not(self.__validate_source_connection__()):
            raise RuntimeError("Source Not Valid %s" % self.source)

    def __validate_source_connection__(self):
        """ Check internet connection """
        test_stock='AAPL'
        try:
            aapl = Options(test_stock, self.source)
            data = aapl.get_all_data()
            return True
        except Exception:
            return False

    def pull_data(self, ticket_list):

        if isinstance(ticket_list, str):
            ticket_list = (ticket_list, )

        option_dict = {}
        for ticket in ticket_list:
            print "Pull Stock : %s ..." % ticket
            try:
                option_method = Options(ticket, self.source)
                option_dict[ticket] = option_method.get_all_data()
            except Exception as err_message:
                option_dict[ticket] = None
                print "Exception: %s" % err_message
                # raise RuntimeError("Can't not download Stock: %s" % stock)
            if len(option_dict[ticket]) == 0:
                print "Length of data is Zero during the time asked"
        return option_dict


class StockSqlObj(object):
    """Class used to build mapping from table to sql"""
    pass


class OptionSQLUtil(SQLUtil):

    def __init__(self, username, password, database, host="localhost", dialect="mysql",
                 driver="mysqldb", port="3306"):
        # a sqlalchemy engine connection URL could be constructed as:
        # dialect+driver://username:password@host:port/database
        # engine = sqlalchemy.create_engine('mysql://root:000539@localhost:3306/test')

        db_connection_string = dialect + '+' + driver + '://' + username + ":" + \
            password + "@" + host + ":" + port + "/" + database
        try:
            engine = sqlalchemy.create_engine(db_connection_string)
        except:
            raise RuntimeError("Can not create engine for URL: %s" % db_connection_string)
        SQLUtil.__init__(self, engine)
        self.metadata = MetaData(self.engine)

    def create_option_table(self, table_name=None):
        if table_name is None:
            raise ValueError('Table name can not be None')

        metadata = MetaData(self.engine)
        # Index([u'Last', u'Bid', u'Ask', u'Chg', u'PctChg', u'Vol',
        #       u'Open_Int', u'IV', u'Root', u'IsNonstandard', u'Underlying',
        #       u'Underlying_Price', u'Quote_Time'], dtype='object')
        my_table = Table(table_name, metadata,
                         Column('Quote_Time'. DATETIME, primary_key=True),
                         Column('Strike', Float, primary_key=True),
                         Column('Expiry', DATETIME, primary_key=True),
                         Column('Type', String(100), primary_key=True),
                         Column('Symbol', String(100)),
                         Column('Last', Float),
                         Column('Bid', Float),
                         Column('Ask', Float),
                         Column('Chg', Float),
                         Column('PctChg', Float),
                         Column('Vol', Float),
                         Column('Open_Int', Float),
                         Column('IV', Float),
                         Column('Root', String(100)),
                         Column('IsNonstandard', String(100)),
                         Column('Underlying', String(100)),
                         Column('Underlying_Price', Float),
                         Column('Quote_Time', DATETIME))

        return my_table

    def build_table_to_sql_mapping(self, table_name):
        """"""
        from sqlalchemy import MetaData, Table
        from sqlalchemy import Column, DATETIME

        metadata = MetaData(self.engine)
        stocktable = Table(table_name, metadata, Column("Date", DATETIME, primary_key=True),autoload=True)
        table_obj = self._map_class_to_some_table_(StockSqlObj, stocktable, table_name)
        session = self.create_session()
        return session, table_obj

    def get_largest_quote_time(self, ticket):
        pass


class OptionServer(OptionSQLUtil):

    def __init__(self, username, password, database, host="localhost", dialect="mysql",
                 driver="mysqldb", port="3306"):
        OptionSQLUtil.__init__(self,  username, password, database, host=host, dialect=dialect,
                               driver=driver, port=port)

    def init_db(self, ticket):
        pass

    def update_db(self, ticket_list):
        print "\n++++++++++++++++++\nUpdate Table Service"

        # change the stock_name into tuple if it is a string
        if isinstance(ticket_list, str):
            ticket_list = (ticket_list, )

        onlinedata = OptionScraper()

        for ticket in ticket_list:
            print "Update table: %s" % ticket
            table_exist = self.check_one_table_avail(ticket)
            print "Table exist: %s" % table_exist
            if table_exist:
                # get end time
                endtime = get_cur_date()
                #print type(endtime), endtime
                # get largest qoute time
                largest_quote_time = self.get_largest_quote_Time(ticket) + datetime.timedelta(days=1)
                #print type(starttime), starttime
                if endtime <= largest_quote_time:
                    print "Table already up-to-date"
                else:
                    option_dict = onlinedata.pull_data(ticket)
                    # write into db
                    if option_dict[dict] is not None:
                        print "Updating..."
                        self.option_to_sql(ticket, option_dict[ticket], if_exists='append')
                    else:
                        print "Online Data is None. Skip it."
            else:
                print "Stock no exist. New table will be written in..."
                # get end time
                # endtime = get_cur_date()
                # print type(endtime), endtime
                # get start time
                # starttime = datetime.datetime(1990, 1, 1)
                # print type(starttime), starttime
                option_dict = onlinedata.pull_data(ticket)
                self.init_db(ticket)

    def option_to_sql(self, ticket, option_df, if_exist='fail'):

        if isinstance(ticket, str):
            raise ValueError('Input Arg: ticket should be one ticket name(string)')

        if if_exist not in ('fail', 'replace', 'append'):
            raise ValueError('if_exist value error')

        session = self.create_session()
        option_table = self.create_option_table()




class OptionClient(OptionSQLUtil):

    def __init__(self, username, password, database, host="localhost", dialect="mysql",
                 driver="mysqldb", port="3306"):
        OptionSQLUtil.__init__(self,  username, password, database, host=host, dialect=dialect,
                              driver=driver, port=port)

    def read_option_record(self):
        pass


def get_cur_date():
    """Get current date. Hour, minute and second will be set to 0"""
    now = datetime.datetime.now()
    return datetime.datetime(now.year, now.month, now.day)