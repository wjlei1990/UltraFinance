#sqlalchemy stubs
def map_class_to_some_table(cls, table, entity_name, **kw):
    from sqlalchemy.orm import mapper
    newcls = type(entity_name, (cls, ), {})
    mapper(newcls, table, **kw)
    return newcls

def create_session(engine):
    from sqlalchemy.orm import sessionmaker
    session = sessionmaker(bind=engine)
    session = session()
    return session

class StockSqlObj(object):
    pass

def loadsession(table_name, engine):
    """"""
    from sqlalchemy import MetaData, Table
    from sqlalchemy import Column, DATETIME

    metadata = MetaData(engine)

    stocktable = Table(table_name, metadata, Column("Date", DATETIME, primary_key=True),autoload=True)

    table_obj = map_class_to_some_table(StockSqlObj, stocktable, table_name)

    session = create_session(engine)
    return session, table_obj
