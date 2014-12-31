#!/usr/bin/env python
# encoding: utf-8

import unittest
from sp500 import sp500
import sqlalchemy
import datetime
from StockData import StockOnline
from StockData import StockServer
from StockData import StockClient

class TestSP500(unittest.TestCase):

    def setUp(self):
        self.sp500 = sp500()
        self.sp500.sp500_standard()
        self.engine = sqlalchemy.create_engine('mysql://root:000539@localhost:3306/test')

    def tearDown(self):
        self.sp500 = None
        self.engine.dispose()

    def test_pullsp500(self):
        stock_info_table = self.sp500.stock_info_table
        if len(stock_info_table) < 400:
            raise Exception('stock number is %d') % len(stock_info_table)

    def test_storesql(self):
        self.sp500.store_to_sql(self.engine)

    def test_readsql(self):
        self.sp500.store_to_sql(self.engine)
        n_stocks = len(self.sp500.stock_info_table)
        self.sp500.read_from_sql(self.engine)
        self.assertEqual(n_stocks, len(self.sp500.stock_info_table), 'Pull method and Read method returns different result')


class TestStockOnline(unittest.TestCase):

    def setUp(self):
        self.stockonline = StockOnline()

    def tearDown(self):
        pass

    def test_PullData(self):
        stock_list = ['AAPL',]
        starttime = datetime.datetime(2014, 12, 1)
        endtime = datetime.datetime(2014, 12, 10)
        stock_table = self.stockonline.pull_data(stock_list,starttime, endtime)
        self.assertEqual(len(stock_list), 1)
        self.assertEqual(len(stock_table['AAPL']), 8)


class TestStockServer(unittest.TestCase):

    def setUp(self):
        self.stockserver = StockServer('root', '000539', 'test')
        self.stockonline = StockOnline()

    def tearDown(self):
        self.stockserver.engine.dispose()

    def test_initdb(self):
        stock_list = ['AAPL',]
        starttime = datetime.datetime(2014, 12, 1)
        endtime = datetime.datetime(2014, 12, 10)
        stock_table = self.stockonline.pull_data(stock_list, starttime, endtime)
        # print stock_table['AAPL']
        self.stockserver.init_db(stock_table, init_mode='replace')

    def test_updatedb(self):
        stock_list = ['IBM',]
        self.stockserver.update_db(stock_list)


class TestStockClient(unittest.TestCase):

    def setUp(self):
        self.stockclient = StockClient('root', '000539', 'test')
        self.stockonline = StockOnline()

    def tearDown(self):
        self.stockclient.engine.dispose()

    def test_ReadStockRecord(self):
        stock_list = ['AAPL',]
        starttime = datetime.datetime(2014, 12, 1)
        endtime = datetime.datetime(2014, 12, 10)
        online_stock_table = self.stockonline.pull_data(stock_list,starttime, endtime)
        sql_stock_table = self.stockclient.read_stock_record(stock_list, starttime, endtime)
        #print sql_stock_table
        self.assertEqual(len(online_stock_table), len(sql_stock_table))

if __name__ == '__main__':

    #suite1 = unittest.TestLoader().loadTestsFromTestCase(TestSP500)
    #suite2 = unittest.TestLoader().loadTestsFromTestCase(TestStockServer)
    #suite3 = unittest.TestLoader().loadTestsFromTestCase(TestStockClient)
    #suite = unittest.TestSuite([suite1, suite2, suite3])
    #unittest.TextTestRunner(verbosity=2).run(suite)

    test_classes_to_run = [TestSP500, TestStockOnline, TestStockServer, TestStockClient]

    loader = unittest.TestLoader()

    suites_list = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)
        print test_class
    print len(suites_list)

    big_suite = unittest.TestSuite(suites_list)

    #print len(big_suite)

    runner = unittest.TextTestRunner(verbosity=2)
    results = runner.run(big_suite)





