import pandas as pd
from ipedstable import IpedsTable
from copy import copy,deepcopy

class IpedsCollection(object):
    ''' A collection of multiple IpedsTables '''
    
    def __init__(self):
        self.meta = {}
        self.merged_table = IpedsTable(df=pd.DataFrame())
        return

    def update_meta(
            self,
            name,
            table=None,
            filepath=None,
            keep_columns=None,
            exclude_imputations=None):
        
        if name not in self.meta.keys():
            self.meta.update({name:{}})
        entry = self.meta[name]
        
        if table:
            if isinstance(table,IpedsTable):
                entry.update({'table':deepcopy(table)})
            else:
                raise TypeError('table must be an instance of IpedsTable')
        
        if filepath:
            entry.update({'filepath':filepath})
                
        if keep_columns:
            if isinstance(keep_columns,list) and ('unitid' not in keep_columns):
                keep_columns.append('unitid')    
            entry.update({'keep_columns':keep_columns}) 
            
        if exclude_imputations:
            entry.update({'exclude_imputations':exclude_imputations})
        return

    def drop_meta(self,name):
        del self.meta[name]
        return 

    def import_all(self):
        for name in self.meta.keys():
            self.import_table(name)
        return

    def import_table(self,name):
        entry = self.meta[name]
        if 'filepath' in entry.keys():
            table = IpedsTable(filepath=entry['filepath'])
            self.update_meta(name,table=table)
            del entry['filepath']
        return

    def clean_all(self,dropna=False):
        for name in self.meta.keys():
            self.clean_table(name,dropna=dropna)
        return 

    def clean_table(self,name,dropna=False):
        print(f"cleaning table {name}")
        self.import_table(name)
        entry = self.meta[name]
        table = entry['table']
        if entry['keep_columns'] == 'all':
            entry['keep_columns'] = table.columns
        table.keep_columns(entry['keep_columns'])
        table.purge_imputations(entry['exclude_imputations'])
        if dropna:
            table.dropna(entry['keep_columns'],how='any')
        return

    def merge_table(self,name,how='outer',keep_table=True):
        print(f"Merging {name} with merged_table")
        self._validate_table_import(name)
        table = self.meta[name]['table']
        if len(self.merged_table) == 0:
            self.merged_table.df = table.df.copy()
        else:
            self.merged_table.df = self.merged_table.df.merge(
                                table.df,
                                how=how,
                                on='unitid',
                                suffixes=('','_'+name))
        if not keep_table: 
            self.drop_meta(name)
        return

    def _validate_table_import(self,name):
        if not 'table' in self.meta[name].keys():
            raise KeyError('Table has not been imported.')
        return 

    def merge_all(self,how='inner',keep_table=True):
        for name in self.meta.keys():
            self.merge_table(name,how=how,keep_table=keep_table)
        return

    def pipeline_all(self,dropna=False,how='inner',keep_table=True):
        names = list(self.meta.keys())
        for name in names:
            self.import_table(name)
            self.clean_table(name,dropna=dropna)
            self.merge_table(name,how=how,keep_table=keep_table)
        return