#!/usr/bin/env python
# -*- coding: utf-8 -*-

# sqlalchemy ORM object(SP500_list)
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from SQL_Util import SQLUtil
import sqlalchemy

Base = declarative_base()
_TicketTableName_ = 'Ticket_List'


class TicketList(Base):

    __tablename__ = _TicketTableName_
    ticket = Column(String(20), primary_key=True)

    def __repr__(self):
        pass

class TicketListSQLUtil(SQLUtil):

    def __init__(self, username, password, database, host="localhost", dialect="mysql",
                 driver="mysqldb", port="3306"):
        db_connection_string = dialect + '+' + driver + '://' + username + ":" + \
            password + "@" + host + ":" + port + "/" + database
        try:
            engine = sqlalchemy.create_engine(db_connection_string)
        except:
            raise RuntimeError("Can not create engine for URL: %s" % db_connection_string)
        SQLUtil.__init__(self, engine)
        self.ticket_table = []

    #def drop_table(self):
    #    SP500_list.__table__.

    def create_ticket_table(self):
        from sqlalchemy import Table
        ticket_table = Table(_TicketTableName_, self.metadata,
                             Column('ticket', String(20), primary_key=True))
        return ticket_table

    def read_from_sql(self):
        from sqlalchemy.orm import mapper
        from sqlalchemy import Table
        session = self.create_session()
        # autoload
        Base.metadata.bind = self.engine

        self.ticket_table = []
        query = session.query(TicketList)
        for ticketqry in query:
            self.ticket_table.append(ticketqry.ticket)

    def update_sql(self, all_ticket_list):

        session = self.create_session()
        #ticket_table = self.create_ticket_table()

        if not self.check_one_table_avail(_TicketTableName_):
            # if the table does not exist, create a new table and set ticket_table to none
            Base.metadata.create_all(self.engine)
            self.ticket_table = []
        else:
            # if the table exists, read from it
            self.read_from_sql()

        num_add = 0
        for ticket in all_ticket_list:
            if ticket not in self.ticket_table:
                print 'Add ticket: %s' %ticket
                num_add += 1
                item = TicketList(ticket=ticket)
                session.add(item)
        session.commit()
        session.close()
        print 'Total ticket added in: %d' % num_add

    def finalize(self):
        self.engine.dispose()
