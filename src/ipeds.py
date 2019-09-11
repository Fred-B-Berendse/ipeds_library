import pandas as pd
import numpy as np 
import os
import csv

class ImputationTypes(object):
    '''
    Imputation types for a PandasTable class
    '''  
    analyst_corrected = 'C'
    carry_forward_procedure = 'P'
    data_not_usable = 'H'
    do_not_know = 'D'
    generated_from_other_values = 'G'
    group_median_procedure = 'L'
    implied_zero = 'Z'
    left_blank = 'B'
    logical_imputation = 'L'
    nearest_neighbor_procedure = 'N'
    not_applicable = 'A'
    ratio_adjustment = 'K'
    reported = 'R'
    
    @classmethod
    def type_to_code(cls,type):
        return cls.__dict__.get(type,None)

    @classmethod
    def code_to_type(cls,code):

        for k,v in ImputationTypes.__dict__.items():
            if k.startswith('_'): continue
            if v == code.upper(): 
                return k
        return None


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

    def drop(self,name):
        try:
            delattr(self,name)
            self.names.remove(name)
        except:
            raise NameError('table name not found')
        return 

    def join_all(self):
        
        for i,name in enumerate(self.names):
            table = getattr(self,name)
            if i == 0:
                merged_df = table.df
            else:
                merged_df = merged_df.join(table.df,rsuffix=name)

        return IpedsTable(df=merged_df)


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
        self.update_index()
        return

    def update_index(self):

        if self.df.index.name != 'unitid':
            try:
                self.df.set_index('unitid', inplace=True)
                self.columns = self.df.columns
            except:
                raise KeyError('Dataframe or file has no "unitid" column')
        self.rows = self.df.index.values 
        return

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
        return

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

    def drop_imputations(self, imputation_types, column_list=None, how='all'):
        '''
        Drops rows where all or any columns have an imputation value in imputation_types
        '''

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