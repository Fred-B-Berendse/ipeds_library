from ipedstable import IpedsTable

class IpedsCollection(object):
    ''' A collection of multiple IpedsTables '''
    

    def __init__(self, table_dict = {}):
        self.names = []
        for name,table in table_dict.items():
            self.add(name,table)
        return

    def add(self,name,table):
        if not isinstance(table,IpedsTable):
            raise TypeError("table must be an instance of IpedsTable.")
        setattr(self,name,table)
        self.names.append(name) 
        return

    def add_from_file(self,filepath):
        pass

    def drop(self,name):
        try:
            delattr(self,name)
            self.names.remove(name)
        except:
            raise NameError('table name not found')
        return 

    def keep_columns(self):
        # slices each table to its list of keep_columns
        pass

    def purge_imputations(self):
        # removes excepted imputations from all tables
        pass

    def dropna_all(self):
        # drops NaN values in all columns in each table
        pass

    def clean_all(self):
        # runs keep_columns, purge_imputations, dropna_all 
        # on all tables
        pass

    def join_all(self):
        for i,name in enumerate(self.names):
            table = getattr(self,name)
            if i == 0:
                merged_df = table.df
            else:
                merged_df = merged_df.join(table.df,rsuffix=name)

        return IpedsTable(df=merged_df)