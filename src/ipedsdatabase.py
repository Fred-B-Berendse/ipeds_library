import psycopg2
from sqlalchemy import create_engine

class IpedsDatabase(object):
    '''Class for interactions between IpedsTables and databases
    '''

    def __init__(self, host, port, user, database):
        self.hoststring = f'postgresql://{user}@{host}:{port}/{database}'
        self.engine = create_engine(self.hoststring)
        return

    def to_sql(self,ipeds_table,db_table_name):
        print(f'writing {ipeds_table} to {db_table_name}')
        ipeds_table.df.to_sql(db_table_name,self.engine,if_exists='replace')
        return

    def from_sql(self):
        # reads a SQL table into an Ipeds table
        # see pandas.read_sql
        pass

    def execute(self,sqlstr):
        return self.engine.execute(sqlstr)

    def close(self):
        self.engine.dispose()
        return
    