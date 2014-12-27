#!
# -*- coding: utf-8 -*-

import pandas.io.data as web
import datetime
import pandas.io
# import MySQLdb
# build up connection with mysql
import sqlalchemy

class StockOnline(object):
    """
    Class Server includes methods that pull data from online resource and store them into database
    """
    def __init__(self, source='yahoo'):
        self.source=source
        if not(self.__validate_source_connection__()):
            raise RuntimeError("Source Not Valid %s" % self.source)

    def __validate_source_connection__(self):
        test_stock='AAPL'
        # set start and end time
        start = datetime.datetime(2012, 1, 1)
        end = datetime.datetime(2012, 1, 27)
        try:
            web.DataReader(test_stock, self.source, start, end)
            return True
        except:
            return False

    def pull_data(self, stock_list, start=datetime.datetime(2012,1,1), end=datetime.datetime(2013,1,1)):
        """
        Function that pulls data from online resource
        :param stock_list: tuple that stores the name list of stocks
        :param start: start time
        :param end: end time
        :return:
        """
        # check start and end
        if start > end:
            raise ValueError("StartTime is larger than EndTime")

        print "\n+++++++++++++++++"
        stock_table = {}
        for stock in stock_list:
            print "Pull Stock : %s ..." % stock
            try:
                stock_table[stock] = web.DataReader(stock, self.source, start, end)
            except:
                stock_table[stock] = None
                raise RuntimeError("Can't not download Stock: %s" % stock)

        return stock_table

class StockServer(object):

    def __init__(self, username, password, database, host="localhost", dialect="mysql",
                 driver="mysqldb", port="3306"):
        # a sqlalchemy engine connection URL could be constructed as:
        # dialect+driver://username:password@host:port/database
        # engine = sqlalchemy.create_engine('mysql://root:000539@localhost:3306/test')

        db_connection_string = dialect + '+' + driver + '://' + username + ":" + \
            password + "@" + host + ":" + port + "/" + database
        try:
            self.engine = sqlalchemy.create_engine(db_connection_string)
        except:
            raise RuntimeError("Can not create engine for URL: %s" % db_connection_string)

    def finalize_engine(self):
        self.engine.dispose()

    def init_db(self, stock_table, init_mode='fail'):
        """
        Function that init all the stock data(pandas DataFrame)into sql
        :param engine: sqlalchemy engine
        :param init_mode: to_sql if_exist variable
        :return:
        """
        print "\n+++++++++++++++++"
        for stock_name, stock_data in stock_table.iteritems():
            print "Init Table Name in db: %s" %stock_name
            if stock_data is None:
                print "Data is None...Skip it"
            else:
                # check if table exists
                table_exist=pandas.io.sql.has_table(stock_name, self.engine)
                print "\tTable exist: %s" % table_exist
                table_exist = False
                if not table_exist:
                    try:
                        stock_data.to_sql(stock_name, self.engine, if_exists=init_mode)
                        print "\tCreate table in database... %s" % stock_name
                        # print data
                    except:
                        raise RuntimeError("Can not init table in database for stock %s" %stock_name)

    def update_db(self, stock_list, engine):
        """
        Function that updates the database to current date
        :param engine:
        :return:
        """
        for stock in stock_list:
            # get end time
            now = datetime.datetime.now()
            end = datetime.datetime(temp_now.year, temp_now.month, temp_now.day)
            # get start time
            # start =

            table_exist=pandas.io.sql.has_table(stock, engine)
            #if table_exist:
                # update
            #else:
                # just return


class StockClient(object):
    """
    Class Client includes method that read all kinds of data from the existing database
    """
    def __init__(self, username, password, database, host="localhost", dialect="mysql",
                 driver="mysqldb", port="3306"):
        # a sqlalchemy engine connection URL could be constructed as:
        # dialect+driver://username:password@host:port/database
        # engine = sqlalchemy.create_engine('mysql://root:000539@localhost:3306/test')

        db_connection_string = dialect + '+' + driver + '://' + username + ":" + \
            password + "@" + host + ":" + port + "/" + database
        try:
            self.engine = sqlalchemy.create_engine(db_connection_string)
        except:
            raise RuntimeError("Can not create engine for URL: %s" % db_connection_string)

    def finalize_engine(self):
        self.engine.dispose()

    def read_full_stock_record(self, stock_list):
        """
        Function that pulls the
        :param stock_list: tuple that stores the stock list
        :return:
        """
        stock_table={}
        boolen_dict = self.check_stock_avail(stock_list)
        for stock in stock_list:
            if boolen_dict[stock]:
                stock_table[stock] = pandas.io.sql.read_sql(stock, self.engine)
            else:
                stock_table[stock] = None
        return stock_table

    def read_stock_record(self, stock_list):
        pass

    def list_stock_in_db(self):
        """
        Function that shows all the stock in the database
        :param engine: sqlalchemy engine
        :return:
        """
        from sqlalchemy import inspect
        inspector = inspect(self.engine)

        return inspector.get_table_names()

    def check_stock_avail(self, stock_list):

        boolen_dict = {}
        db_stock_list = self.list_stock_in_db()
        for stock in stock_list:
            if stock in db_stock_list:
                boolen_dict[stock] = True
            else:
                boolen_dict[stock] = False
        return boolen_dict






