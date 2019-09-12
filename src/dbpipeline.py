import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_DEFAULT
import getpass

class DatabasePipeline(object):
    """Class for executing a series of SQL commands as a pipeline.
    """
    def __init__(self, database, user, host, port):
        """
        Parameters
        ----------
        database - name of database
        user - name of database user
        host - host url or ip address
        port - host port

        Returns
        -------
        None
        """
        self.database = database
        self.user = user
        self.host = host
        self.port = port

        self.open_connection()
        self.c = self.conn.cursor()
        self.steps = []

    def open_connection(self):
        self.conn = psycopg2.connect(
                    database=self.database,
                    user=self.user,
                    host=self.host,
                    port=self.port)

    def add_step(self, query, params=None):
        """Add a query to this pipeline.

        Parameters
        ----------
        query : SQL Query string.
        params : dict of params to format the query with

        Returns
        -------
        None
        """
        self.steps.append((query, params))

    def execute(self):
        """Execute all steps in the pipeline.
        """
        for step, params in self.steps:
            self.c.execute(step, params)

        self.conn.commit()

    def close(self):
        self.c.close()
        self.conn.close()



