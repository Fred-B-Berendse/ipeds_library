import pandas as pd
import csv

class IpedsTable(object):
    ''' An object containing data and methods from a single IPEDS data table.'''
    
    def __init__(self, df=None, filepath=None):
        self.df = df
        if isinstance(df,pd.DataFrame):
            self.df = df       
        elif filepath:
            self.df = pd.read_csv(filepath,encoding="cp1252")
        else:
            raise ValueError('IpedsTable requires either a Pandas DataFrame or a filename')
        self.update_columns()
        return

    def __len__(self):
        return len(self.df)

    def info(self,**kwargs):
        return self.df.info(**kwargs) 
    
    def describe(self,**kwargs):
        return self.df.describe(**kwargs)

    def head(self,n=5):
        return self.df.head(n)

    def tail(self,n=5):
        return self.df.tail(n)

    def update_columns(self):
        self.df.columns = [c.strip().lower() for c in self.df.columns]
        self.columns = self.df.columns
        return

    def keep_columns(self, column_list):
        columns_to_drop = (self.columns).difference(column_list).values
        self.drop_columns(columns_to_drop)
        return

    def drop_columns(self, column_list):
        self.df.drop(columns=column_list, inplace=True)
        self.columns = self.df.columns
        return

    def extended_column_list(self, column_list=None):
        if column_list:
            return column_list + self.get_imputation_columns(column_list)
        else:
            return self.df.columns 

    def get_imputation_columns(self, column_list=None):
        col_list = column_list if column_list else self.df.columns
        imputation_columns = ['x'+c for c in col_list if 'x'+c in self.columns]
        return list(set(imputation_columns))

    def dropna(self, column_list, how='all'):
        if how.lower() not in ['all','any']:
            raise ValueError('Invalid method. Valid methods are "all" and "any".')
            
        self.df.dropna(axis=0, how=how, subset=column_list, inplace=True)
        self.rows = self.df.index.values
        return

    def purge_imputations(self, imputation_types, column_list=None, how='all'):
        if how.lower() not in ['all','any']:
            raise ValueError('Invalid method. Valid methods are "all" and "any".')
        
        imputation_columns = self.get_imputation_columns(column_list)
        data_slice = self.df.loc[:,imputation_columns]
        notin_imputation_list = data_slice.applymap(lambda v: v not in imputation_types)

        if how.lower() == 'all':
            row_mask = notin_imputation_list.all(axis=1)
        else: 
            row_mask = notin_imputation_list.any(axis=1)
        
        self.df = self.df[row_mask]
        self.rows = self.df.index.values
        return

    def write_csv(self,filepath):
        self.df.to_csv(
                filepath, 
                header=True, 
                index_label='unitid', 
                quoting=csv.QUOTE_NONNUMERIC
                )
        return