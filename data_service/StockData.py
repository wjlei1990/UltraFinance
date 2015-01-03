#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas.io.data as web
import datetime
import pandas.io
import pandas.io.sql
import sqlalchemy

from SQL_Util import SQLUtil


class StockSqlObj(object):
    """Class used to build mapping from table to sql"""
    pass


class StockSQLUtil(SQLUtil):

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

    def get_largest_date(self, stock_name):
        from sqlalchemy.sql import func

        if not isinstance(stock_name, str):
            raise ValueError("Input arg is not correcty: stock_name")

        session, stockobj = self.build_table_to_sql_mapping(stock_name)
        qry = session.query(func.max(stockobj.Date).label("max_date"))
        res = qry.one()
        return res.max_date

    def build_table_to_sql_mapping(self, table_name):
        """"""
        from sqlalchemy import MetaData, Table
        from sqlalchemy import Column, DATETIME

        metadata = MetaData(self.engine)
        stocktable = Table(table_name, metadata, Column("Date", DATETIME, primary_key=True),autoload=True)
        table_obj = self._map_class_to_some_table_(StockSqlObj, stocktable, table_name)\

        session = self.create_session()
        return session, table_obj

class StockOnline(object):
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
        # set start and end time
        start = datetime.datetime(2012, 1, 1)
        end = datetime.datetime(2012, 1, 27)
        try:
            web.DataReader(test_stock, self.source, start, end)
            return True
        except Exception:
            return False

    def pull_data(self, stock_list, start=datetime.datetime(1990,1,1), end=None):
        """
        Function that pulls data from online resource
        :param stock_list: tuple that stores the name list of stocks
        :param start: start time
        :param end: end time
        :return: dict as [stock_name: DataFrame, ... ]
        """
        # check start and end
        if end is None:
            end = get_cur_date()
        if start > end:
            raise ValueError("StartTime is larger than EndTime")
        # check if stock_list is a string or a list
        if isinstance(stock_list, str):
            stock_list = (stock_list, )

        # print "\n+++++++++++++++++\nPull Data Service"
        stock_dict = {}
        for stock in stock_list:
            print "Pull Stock : %s ..." % stock
            try:
                stock_dict[stock] = web.DataReader(stock, self.source, start, end)
            except Exception as err_message:
                stock_dict[stock] = None
                print "Exception: %s" % err_message
                # raise RuntimeError("Can't not download Stock: %s" % stock)
            if len(stock_dict[stock]) == 0:
                print "Length of data is Zero during the time asked"
        return stock_dict


class StockServer(StockSQLUtil):

    def __init__(self, username, password, database, host="localhost", dialect="mysql",
                 driver="mysqldb", port="3306"):
        StockSQLUtil.__init__(self,  username, password, database, host=host, dialect=dialect,
                              driver=driver, port=port)

    def update_index_table(self):
        #index_list = {'^GSPC': 'SP500', '^IXIC': 'NASDAQ', '^RUT': 'RUSSEL2000'}
        index_list = ['^GSPC', '^IXIC', '^RUT']
        #index_table = StockOnline().pull_data(index_list.keys())
        # change table name since sql won't take table name starting with '^'
        #for ticket, table_name in index_list.iteritems():
        #    index_table[table_name] = index_table.pop(ticket)
        self.update_db(index_list)

    def init_db(self, stock_table, init_mode='fail'):
        """
        Function that init all the stock data(pandas DataFrame)into sql
        :param engine: sqlalchemy engine
        :param init_mode: to_sql if_exist variable
        :return:
        """
        if not isinstance(stock_table, dict):
            raise ValueError("first argument should be a dict")

        print "\n++++++++++++++++\nInit DB Service"
        for stock_name, stock_data in stock_table.iteritems():
            print "Init Table Name in db: %s" % stock_name
            if stock_data is None:
                print "Data is None...Skip it"
            else:
                # check if table exists
                table_exist = self.check_one_table_avail(stock_name)
                print "\tTable exist: %s" % table_exist
                table_exist = False
                if not table_exist:
                    try:
                        new_stock_name = stock_name
                        new_stock_name.replace('^', '_')
                        stock_data.to_sql(new_stock_name, self.engine, if_exists=init_mode)
                        print "\tCreate table in database... %s" % stock_name
                        # print data
                    except:
                        print ("Can not init table in database for stock %s" % stock_name)
                else:
                    print "Table already exist. Skip it. But you can force to replace it by setting init_mode=replace"

    def update_db(self, stock_list):
        """
        Function that updates the database to current date
        :param engine:
        :return:
        """
        print "\n++++++++++++++++++\nUpdate Table Service"

        # change the stock_name into tuple if it is a string
        if isinstance(stock_list, str):
            stock_list = (stock_list, )

        onlinedata = StockOnline()

        for stock_name in stock_list:
            print "Update table: %s" %stock_name
            table_exist = self.check_one_table_avail(stock_name)
            print "Table exist: %s" % table_exist
            if table_exist:
                # get end time
                endtime = get_cur_date()
                #print type(endtime), endtime
                # get start time
                starttime = self.get_largest_date(stock_name) + datetime.timedelta(days=1)
                #print type(starttime), starttime
                if endtime <= starttime:
                    print "Table already up-to-date"
                else:
                    stock_table = onlinedata.pull_data(stock_name, starttime, endtime)
                    # write into db
                    if stock_table[stock_name] is not None:
                        print "Updating..."
                        new_stock_name = stock_name
                        new_stock_name.replace('^', '_')
                        stock_table[stock_name].to_sql(new_stock_name, self.engine, if_exists='append')
                    else:
                        print "Online Data is None. Skip it."
            else:
                print "Stock no exist. New table will be written in..."
                # get end time
                endtime = get_cur_date()
                print type(endtime), endtime
                # get start time
                starttime = datetime.datetime(1990, 1, 1)
                print type(starttime), starttime
                stock_table = onlinedata.pull_data(stock_name, starttime, endtime)
                self.init_db(stock_table)


class StockClient(StockSQLUtil):
    """
    Class Client includes method that read all kinds of data from the existing database
    """

    def __init__(self, username, password, database, host="localhost", dialect="mysql",
                 driver="mysqldb", port="3306"):
        StockSQLUtil.__init__(self,  username, password, database, host=host, dialect=dialect,
                              driver=driver, port=port)

    def read_full_record(self, stock_list):
        """
        Function that pulls the the full record of stocks in the list.
        Attention: the return df is different from other methods(pull_data, or read part). The \
                method we us is from pandas. Have no idea what is happening inside. The \
                read_stock_record method is more recommended
        :param stock_list: list that stores the stock names
        :return:
        """
        if isinstance(stock_list, str):
            stock_list = (stock_list, )

        stock_dict = {}
        bool_dict = self.check_table_list_avail(stock_list)
        for stock in stock_list:
            if bool_dict[stock]:
                stock_dict[stock] = pandas.io.sql.read_sql(stock, self.engine, index_col='Date')
            else:
                stock_dict[stock] = None
        return stock_dict

    def read_stock_record(self, stock_list, starttime=None, endtime=None):
        """
        Read stock data from database based on user's request
        :param stock_list:
        :param starttime:
        :param endtime:
        :return: stock dict as [stock_name: dataframe, ...]
        """
        from sqlalchemy import inspect, and_

        if isinstance(stock_list, str):
            stock_list = (stock_list, )

        if starttime is None:
            starttime = datetime.datetime(2012, 1, 1)
        if endtime is None:
            endtime = get_cur_date()

        stock_dict = {}

        if starttime > endtime:
            raise ValueError('Start time is larger than end time')

        for stock_name in stock_list:
            session, stockobj = self.build_table_to_sql_mapping(stock_name)
            if self.check_one_table_avail(stock_name):
                # for key in stockobj.__table__.columns:
                #    print key

                # inspector = inspect(stockobj)
                # for column in inspector.attrs:
                #    print column.key
                qry = (session.query(stockobj).filter(and_(stockobj.Date >= starttime, stockobj.Date <= endtime)))
                # print qry.first()
                stock_dict[stock_name] = self.convert_qry_to_DF(qry)
            else:
                print "Stock %s not exist. return None." % stock_name
                stock_dict[stock_name] = None
        return stock_dict

    def read_recent_stock_record(self, stock_list, ndays=90):
        """
        Read stock data from database for previous ndays
        :param stock_list:
        :param starttime:
        :param endtime:
        :return: stock dict as [stock_name: dataframe, ...]
        """
        from time import time
        from sqlalchemy import inspect, and_

        if isinstance(stock_list, str):
            stock_list = (stock_list, )

        # stock_dict1 = {}
        # query method
        #t1 = time()
        #for ticket in stock_list:
        #    query = '(select * from %s ORDER by Date DESC limit %d) ORDER by Date ASC;' %(ticket, ndays)
        #    stock_dict1[ticket] = pandas.read_sql_query(query, self.engine)

        # sqlalchemy method
        #t2 = time()
        stock_dict2 = {}
        for ticket in stock_list:
            session, stockobj = self.build_table_to_sql_mapping(ticket)
            qry = session.query(stockobj).order_by(stockobj.Date.desc()).limit(ndays)
            stock_dict2[ticket] = self.convert_qry_to_DF(qry, order='reverse')
        #t3 = time()
        #print t2-t1, t3-t2
        #endtime = get_cur_date()
        #starttime = endtime - datetime.timedelta(days=ndays)
        #stock_dict = self.read_stock_record(stock_list, starttime, endtime)

        return stock_dict2

    @staticmethod
    def convert_qry_to_DF(qry, order='normal'):
        """
        convert sqlalchemy query into pandas dataframe
        :param qry:
        :return:
        """
        # pandas dataframe is based on dict on columns
        stock_table = {}
        index_list = []
        stock_table['Open']=[]
        stock_table['High']=[]
        stock_table['Low']=[]
        stock_table['Close']=[]
        stock_table['Volume']=[]
        stock_table['Adj Close']=[]
        # print order
        if order in ('normal', 'Normal'):
            # transfer in normal mode
            for idx, entry in enumerate(qry.all()):
                # print idx, entry.__dict__['Adj Close']
                stock_table['Open'].append(entry.Open)
                stock_table['High'].append(entry.High)
                stock_table['Low'].append(entry.Low)
                stock_table['Close'].append(entry.Close)
                stock_table['Volume'].append(entry.Volume)
                stock_table['Adj Close'].append(entry.__dict__['Adj Close'])
                index_list.append(entry.Date)
        elif order in ('reverse', 'Reverse'):
            # transfer in reverse mode
            for idx, entry in enumerate(qry.all()):
                #print idx, entry.__dict__['Adj Close']
                stock_table['Open'].insert(0, entry.Open)
                stock_table['High'].insert(0, entry.High)
                stock_table['Low'].insert(0, entry.Low)
                stock_table['Close'].insert(0, entry.Close)
                stock_table['Volume'].insert(0, entry.Volume)
                stock_table['Adj Close'].insert(0, entry.__dict__['Adj Close'])
                index_list.insert(0, entry.Date)
        else:
            raise ValueError('Input Arg: order wrong')

        df_column_order = ['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']
        #convert to DataFrame
        df=pandas.DataFrame(stock_table, index=index_list, columns=df_column_order)
        df.index.name = 'Date'
        return df

def get_cur_date():
    """Get current date. Hour, minute and second will be set to 0"""
    now = datetime.datetime.now()
    return datetime.datetime(now.year, now.month, now.day)