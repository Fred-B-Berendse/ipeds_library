import psycopg2
import sqlalchemy as db 
from ipedstable import IpedsTable


class IpedsDatabase(object):
    '''Class for interactions between IpedsTables and databases
    '''

    def __init__(self, host, port, user, database):
        self.hoststring = f'postgresql://{user}@{host}:{port}/{database}'
        self.engine = db.create_engine(self.hoststring)
        return

    def to_sql(self, ipeds_table, db_table_name):
        print(f'writing {ipeds_table} to {db_table_name}')
        ipeds_table.df.to_sql(db_table_name, self.engine, if_exists='replace')
        return

    def from_sql(self,table_name):
        # reads a SQL table into an IpedsTable object
        # see pandas.read_sql
        print(f'reading {table_name} from database')
        metadata = db.MetaData()
        table = db.Table(table_name, metadata, autoload=True, autoload_with=self.engine)
        query = db.select([table])
        return self.from_sql_query(query)

    def from_sql_query(self,sql_str):
        # reads a SQL query into an IpedsTable
        # https://towardsdatascience.com/sqlalchemy-python-tutorial-79a577141a91
        ResultProxy = self.engine.execute(sql_str)
        ResultSet = ResultProxy.fetchall()
        result = IpedsTable(data=ResultSet,columns=ResultSet[0].keys())
        result.df.drop(columns=['index'],inplace=True)
        result.update_columns()
        return result

    def close(self):
        self.engine.dispose()
        return
