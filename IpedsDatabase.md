# IpedsDatabase class
A class for interactions between IpedsTables and databases. This class utilizes [SQLAlchemy](https://www.sqlalchemy.org/) for making database connections.

## Object Attributes
### hoststring
    A formatted connection string used by the SQLAlchemy engine.
### engine
    A SQLAlchemy database connection engine

## Object Methods
### \__init__(self, host, port, user, database)
    Instantiates the class

### to_sql(self,ipeds_table,db_table_name)
    Writes an IpedsTable object as a table to the database

### from_sql(self) (not yet implemented)
    Reads a SQL table and returns an IpedsTable object

### from_sql_query(self,sql_str)
    Executes a SQL query on the database. Returns a SQLAlchemy cursor for containing the results of the query.

### close(self)
    Closes the connection