from ipedstable import IpedsTable
from copy import deepcopy

class IpedsCollection(object):
    ''' A collection of multiple IpedsTables '''
    

    def __init__(self):
        self.meta = {}
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

    def clean_tables(self,dropna=False):
        for name in self.meta.keys():
            print(f"processing table {name}")
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

    def join_all(self):
        self._validate_table_imports()
        merged_df = None
        for name,entry in self.meta.items():
            print(f'Merging {name}')
            table = entry['table']
            if merged_df is None:
                merged_df = table.df.copy()
            else:
                merged_df = merged_df.merge(
                        table.df,
                        on='unitid',
                        suffixes=('','_'+name))
        return IpedsTable(df=merged_df)

    def _validate_table_imports(self):
        if not all(['table' in entry.keys() for entry in self.meta.values()]):
            raise KeyError('One or more tables has not been imported.')
        return 
