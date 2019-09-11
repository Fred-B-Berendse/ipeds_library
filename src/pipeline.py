from imputationtypes import ImputationTypes
from ipedstable import IpedsTable
from ipedscollection import IpedsCollection

class Pipeline(object):
    """Class for executing a series of SQL commands as a pipeline.
    """
    def __init__(self, conn, ):
        """
        Parameters
        ----------
        conn : SQL connection object
        Returns
        -------
        None
        """
        self.conn = conn
        self.c = conn.cursor()
        self.steps = []

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
        self.conn.close()


if __name__ == '__main__':
    
    exclude_list = [
            ImputationTypes.data_not_usable,
            ImputationTypes.do_not_know,
            ImputationTypes.left_blank,
            ImputationTypes.not_applicable
            ]

    adm2017 = IpedsTable(filepath='data/adm2017.csv')
    hd2017 = IpedsTable(filepath='data/hd2017.csv')
    column_list = ['enrlpt']
    adm2017.drop_imputations(exclude_list,column_list,how='any')
    adm2017.write_csv('data/test.csv')
    tables = {
            'hd2017':hd2017,
            'adm2017':adm2017
            }
    table_collection = IpedsCollection(table_dict=tables)
    merged_table = table_collection.join_all()