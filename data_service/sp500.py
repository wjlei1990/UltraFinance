#!/usr/bin/env python
# -*- coding: utf-8 -*-

# sqlalchemy ORM object(SP500_list)
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import engine
Base = declarative_base()

_SP500_table_name_ = 'SP500_list'


class SP500_list(Base):

    __tablename__ = _SP500_table_name_

    ticket = Column(String(20), primary_key=True)
    company_name = Column(String(200), nullable=False)
    sector = Column(String(50), nullable=True)
    sales = Column(Float, nullable=True)
    sales_prior = Column(Float, nullable=True)
    oper_per_share = Column(Float, nullable=True)
    oper_per_share_prior = Column(Float, nullable=True)
    oper_per_share_rep = Column(Float, nullable=True)
    oper_per_share_rep_prior = Column(Float, nullable=True)

    def __repr__(self):
        pass

    def get_table_name(self):
        return self.__tablename__


class sp500(object):

    def __init__(self, source='sp500_standard'):
        self.source = source
        self.method_list = {'finsymbol': self.finsymbol, 'sp500_standard': self.sp500_standard}
        self.pull_data = self.method_list[self.source]
        self.stock_info_table = {}

    @staticmethod
    def finsymbol():
        import finsymbols
        stock_list = []
        stock_info = finsymbols.get_sp500_symbols()
        for i in range(len(stock_info)):
            stock_list.append(stock_info[i]['symbol'])
        return stock_list

    def sp500_standard(self):
        import urllib

        stock_info_table = {}
        source = "http://www.spindices.com/documents/additional-material/sp-500-eps-est.xlsx?force_download=true"
        xlspath = '/tmp/sp500.xls'
        urllib.urlretrieve(source, xlspath)
        import xlrd
        stockxls = xlrd.open_workbook(xlspath)

        # print workbook.sheet_names()
        worksheet = stockxls.sheet_by_name('ISSUES')
        num_rows = worksheet.nrows - 1

        for curr_row in range(6, num_rows,1):
            curr_row += 1
            # row = worksheet.row(curr_row)
            ticket=worksheet.cell_value(curr_row, 0)
            # print "ticket: %s" % ticket
            stock_info_table[ticket] = []
            for curr_column in range(1,9):
                # Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
                # cell_type = worksheet.cell_type(curr_row, curr_cell)
                cell_value = worksheet.cell_value(curr_row, curr_column)
                stock_info_table[ticket].append(cell_value)
        self.stock_info_table = stock_info_table

    def get_sp500_ticket_list(self):
        return self.stock_info_table.keys()

    def store_to_sql(self, engine):

        from sqlalchemy.orm import sessionmaker
        DB_Session = sessionmaker(bind=engine)
        session = DB_Session()
        # conn = engine.connect()

        # delete old tables if it exists
        from StockData import SQLUtil
        from sqlalchemy import text
        util = SQLUtil(engine)
        if util.check_one_table_avail(_SP500_table_name_):
            # cmd = 'delete table ' + _SP500_table_name_
            session.query(SP500_list).delete()
        else:
            Base.metadata.create_all(engine)
            #Base.metadata.tables[]

        for ticket, info in self.stock_info_table.iteritems():
            stock = SP500_list(ticket=ticket, company_name=info[0], sales=info[1],
                               sales_prior=info[2], oper_per_share=info[3], oper_per_share_prior=info[4],
                               oper_per_share_rep=info[5], oper_per_share_rep_prior=info[6],
                               sector=info[7])
            session.add(stock)

        session.commit()
        session.close()

    def read_from_sql(self, engine):
        from sqlalchemy.orm import sessionmaker
        DB_Session = sessionmaker(bind=engine)
        session = DB_Session()
        Base.metadata.bind = engine

        query = session.query(SP500_list)
        for stock in query:
            self.stock_info_table[stock.ticket]=[stock.company_name, stock.sales, stock.sales_prior,
                        stock.oper_per_share, stock.oper_per_share_prior, stock.oper_per_share_rep,
                        stock.oper_per_share_rep_prior, stock.sector]
        session.close()















