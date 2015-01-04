from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.types import DATETIME
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, MetaData
import sqlalchemy
import datetime

from pandas.io.data import Options
from sqlalchemy import *

from SQL_Util import SQLUtil

Base = declarative_base()


class OptionScraper(object):
    """
    Class includes methods that pull data from online resource
    """
    def __init__(self, source='yahoo', check_connection=False):
        if source not in ['yahoo', 'google', 'fred']:
            raise ValueError('Current Source only supports: yahoo, google and fred')
        self.source = source
        if check_connection:
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
            print "Pull Option Data : %s ..." % ticket
            try:
                option_method = Options(ticket, self.source)
                option_dict[ticket] = option_method.get_all_data()
                if len(option_dict[ticket]) == 0:
                    print "Length of data is Zero during the time asked"
            except Exception as err_message:
                option_dict[ticket] = None
                print "Exception: %s" % err_message
                # raise RuntimeError("Can't not download Stock: %s" % stock)
        return option_dict


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

    def create_option_table(self, table_name=None):

        if not isinstance(table_name, str):
            raise ValueError('Table name can not be None')

        # print "table_name: ", table_name
        # metadata = MetaData(self.engine)
        # Index([u'Last', u'Bid', u'Ask', u'Chg', u'PctChg', u'Vol',
        #       u'Open_Int', u'IV', u'Root', u'IsNonstandard', u'Underlying',
        #       u'Underlying_Price', u'Quote_Time'], dtype='object')
        my_table = Table(table_name, self.metadata,
                         Column('Quote_Time', DATETIME, primary_key=True),
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
                         Column('Underlying_Price', Float))
        return my_table

    class SqlObj(object):
        """Class used to build mapping from table to sql"""
        pass

    def build_table_to_sql_mapping(self, table_name, table_class):
        """"""
        from sqlalchemy import MetaData, Table
        from sqlalchemy import Column, DATETIME

        stocktable = Table(table_name, self.metadata, autoload=True)
        table_obj = self._map_class_to_some_table_(table_class, stocktable, table_name)
        session = self.create_session()
        return session, table_obj

    def get_largest_quote_time(self, table_name):
        from sqlalchemy.sql import func

        if not isinstance(table_name, str):
            raise ValueError("Input arg is not correcty: ticket_name")

        session, tableobj = self.build_table_to_sql_mapping(table_name, self.SqlObj)
        qry = session.query(func.max(tableobj.Quote_Time).label("max_date"))
        res = qry.one()
        session.close()
        return res.max_date


class OptionServer(OptionSQLUtil):

    def __init__(self, username, password, database, host="localhost", dialect="mysql",
                 driver="mysqldb", port="3306"):
        OptionSQLUtil.__init__(self,  username, password, database, host=host, dialect=dialect,
                               driver=driver, port=port)

    def update_db(self, ticket_list):
        print "\n++++++++++++++++++\nUpdate Table Service"

        # change the stock_name into tuple if it is a string
        if isinstance(ticket_list, str):
            ticket_list = (ticket_list, )
        if not isinstance(ticket_list, (list, tuple)):
            raise ValueError('Input Arg not Right')

        onlinedata = OptionScraper()

        for ticket in ticket_list:
            print "Update table: %s" % ticket
            table_exist = self.check_one_table_avail(ticket)
            print "Table exist: %s" % table_exist
            if table_exist:
                # get end time
                now = get_cur_date()
                #print type(endtime), endtime
                # get largest qoute time
                largest_quote_time = self.get_largest_quote_time(ticket)
                if compare_date(largest_quote_time, now) == 0:
                # print type(starttime), starttime
                # if endtime <= largest_quote_time:
                    print "Table already up-to-date"
                elif compare_date(largest_quote_time, now) == -1:
                    print "Get data..."
                    option_dict = onlinedata.pull_data(ticket)
                    # write into db
                    if option_dict[dict] is not None:
                        print "Updating..."
                        self.option_to_sql(ticket, option_dict[ticket], if_exist='append')
                    else:
                        print "Online Data is None. Skip it."
                else:
                    raise ValueError('Date Wrong. Most Recent Date in database is larger than today')
            else:
                print "Stock no exist. New table will be written in..."
                # get end time
                # endtime = get_cur_date()
                # print type(endtime), endtime
                # get start time
                # starttime = datetime.datetime(1990, 1, 1)
                # print type(starttime), starttime
                option_dict = onlinedata.pull_data(ticket)
                # option_table = self.create_option_table(ticket)
                # option_table.create(self.engine)
                print 'Updating database...'
                self.option_to_sql(ticket, option_dict[ticket])

    def option_to_sql(self, ticket, option_df, if_exist='fail'):

        from sqlalchemy.orm import mapper
        if not isinstance(ticket, str):
            raise ValueError('Input Arg: ticket should be one ticket name(string)')

        if if_exist not in ('fail', 'replace', 'append'):
            raise ValueError('if_exist value error')

        option_table = self.create_option_table(ticket)
        session, optionobj = self.build_table_to_sql_mapping(ticket, self.SqlObj)

        if not self.check_one_table_avail(ticket):
            option_table.create(self.engine, checkfirst=True)
        else:
            if if_exist == 'fail':
                # fail mode: if tables exists, just return
                print "Mode Fail: if table exists, just return"
                return
            elif if_exist == 'replace':
                option_table.drop(self.engine, checkfirst=True)
                option_table.create(self.engine, checkfirst=True)

        for i in range(len(option_df)):
            #print 'Add...', i
            # Column('Strike', Float, primary_key=True),
            # Column('Expiry', DATETIME, primary_key=True),
            # Column('Type', String(100), primary_key=True),
            #print str(option_table.insert())
            #print "type:", type(option_df)
            # Index([u'Last', u'Bid', u'Ask', u'Chg', u'PctChg', u'Vol',
            #       u'Open_Int', u'IV', u'Root', u'IsNonstandard', u'Underlying',
            #       u'Underlying_Price', u'Quote_Time'], dtype='object')
            #print "value:", option_df['Quote_Time'][i], option_df.index.values[i][0],option_df.index.values[i][1],option_df.index.values[i][2]
            #print "value:", type(option_df['Quote_Time'][i]), type(option_df.index.values[i][0]),type(option_df.index.values[i][1]),type(option_df.index.values[i][2])
            values = option_df.values[i]

            trans = self.conn.begin()
            try:
                ins = option_table.insert().values(Quote_Time=option_df.values[i][12], Strike=option_df.index.values[i][0],
                                                   Expiry=option_df.index.values[i][1], Type=option_df.index.values[i][2],
                                                   Last=values[0], Bid=values[1], Ask=values[2], Chg=values[3], PctChg=values[4],
                                                   Vol=values[5], Open_Int=values[6], IV=values[7], Root=values[8],
                                                   IsNonstandard=values[9], Underlying=values[10], Underlying_Price=values[11])
                self.conn.execute(ins)
                trans.commit()
            except:
                trans.rollback()
                raise

        #session.commit()
        #session.close()


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


def compare_date(date1, date2):
    temp_date1=datetime.datetime(date1.year, date1.month, date1.day)
    temp_date2=datetime.datetime(date2.year, date2.month, date2.day)
    if temp_date1 < temp_date2:
        return -1
    elif temp_date1 > temp_date2:
        return 1
    else:
        return 0