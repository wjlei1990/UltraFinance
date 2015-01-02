from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.types import DATETIME
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, MetaData
from pandas.io.data import Options

Base = declarative_base()

def create_option_table(table_name=None, engine=None):
    if table_name is None:
        raise ValueError('Table name can not be None')

    metadata = MetaData(engine)
    #Index([u'Last', u'Bid', u'Ask', u'Chg', u'PctChg', u'Vol', u'Open_Int', u'IV', u'Root', u'IsNonstandard', u'Underlying', u'Underlying_Price', u'Quote_Time'], dtype='object')
    my_table = Table(table_name, metadata,
                    Column('Strike', Float, primary_key=True),
                    Column('Expiry', DATETIME, primary_key=True),
                    Column('Type', String(100), primary_key=True),
                    Column('Quote_Time'. DATETIME, primary_key=True),
                    Column('Symbol', String(100), primary_key=Flase),
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
                    Column('Quote_Time', DATETIME),
                    )

    return my_table


class OptionScraper():
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


class OptionServer(object):
    pass


class OptionClient(object):
    pass