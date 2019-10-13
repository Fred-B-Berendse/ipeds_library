import pandas as pd
from ipedstable import IpedsTable
from copy import copy, deepcopy


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
            map_values=None,
            col_levels = None,
            filter_values = None,
            exclude_imputations=None):

        if name not in self.meta.keys():
            self.meta.update({name: {}})
        entry = self.meta[name]

        if table:
            if isinstance(table, IpedsTable):
                entry.update({'table': deepcopy(table)})
            else:
                raise TypeError('table must be an instance of IpedsTable')

        if filepath:
            entry.update({'filepath': filepath})

        if keep_columns:
            if isinstance(keep_columns, list)\
                    and ('unitid' not in keep_columns):
                keep_columns.append('unitid')
            entry.update({'keep_columns': keep_columns})

        if col_levels:
            entry.update({'col_levels': col_levels})

        if filter_values:
            entry.update({'filter_values': filter_values})

        if map_values:
            entry.update({'map_values': map_values})

        if exclude_imputations:
            entry.update({'exclude_imputations': exclude_imputations})
        return

    def drop_meta(self, name):
        del self.meta[name]
        return

    def import_all(self):
        for name in self.meta.keys():
            self.import_table(name)
        return

    def import_table(self, name):
        entry = self.meta[name]
        if 'filepath' in entry.keys():
            table = IpedsTable(filepath=entry['filepath'])
            self.update_meta(name, table=table)
            del entry['filepath']
        return

    def make_multicols_all(self):
        for name in self.meta.keys():
            print(f"making multicols for {name}")
            self.make_multicols(name)
        return
       
    def make_multicols(self, name):
        entry = self.meta[name]
        self._validate_table_import(name)
        if 'col_levels' in entry.keys():
            table = entry['table']
            col_levels = copy(entry['col_levels'])
            if 'unitid' in entry['col_levels']:
                col_levels.remove('unitid')
            col_levels.insert(0,'unitid')
            table.make_multicols(col_levels)
        return
 
    def clean_all(self, dropna=False):
        for name in self.meta.keys():
            self.clean_table(name, dropna=dropna)
        return

    def clean_table(self, name, dropna=False):
        print(f"cleaning table {name}")
        self.import_table(name)
        entry = self.meta[name]
        table = entry['table']
        if entry['keep_columns'] == 'all':
            entry['keep_columns'] = table.columns
        table.keep_columns(entry['keep_columns'])
        table.purge_imputations(entry['exclude_imputations'], how='all')
        if dropna:
            table.dropna(entry['keep_columns'], how='any')
        return

    def filter_all(self):
        for name in self.meta.keys():
            self.filter_values(name)
        return

    def filter_values(self, name):
        print(f"filtering values in table {name}")
        self._validate_table_import(name)
        entry = self.meta[name]
        if 'filter_values' in entry.keys():
            table = entry['table']
            table.filter_values(entry['filter_values'])
        return

    def map_values_all(self):
        for name in self.meta.keys():
            self.map_values(name)
        return

    def map_values(self, name):
        print(f"mapping values in table {name}")
        entry = self.meta[name]
        if 'map_values' in entry.keys():
            table = entry['table']
            table.map_values(entry['map_values'])
        return

    def merge_table(self, name, how='inner', keep_table=True):
        print(f"Merging {name} with merged_table")
        self._validate_table_import(name)
        table = deepcopy(self.meta[name]['table'])
        if len(self.merged_table) == 0:
            self.merged_table.df = table.df
        else:
            self.merged_table.df = self.merged_table.df.merge(
                                table.df,
                                how=how,
                                on='unitid',
                                suffixes=('', '_'+name))
        if not keep_table:
            self.drop_meta(name)
        return

    def _validate_table_import(self, name):
        if 'table' not in self.meta[name].keys():
            raise KeyError('Table has not been imported.')
        return

    def merge_all(self, how='inner', keep_table=True):
        for name in self.meta.keys():
            self.merge_table(name, 
                             how=how,  
                             keep_table=keep_table)
        return

    def pipeline_all(self, dropna=False, how='inner', keep_table=True):
        self.import_all()
        self.clean_all()
        self.map_values_all()
        self.filter_all()
        self.make_multicols_all()
        self.merge_all(how=how, keep_table=keep_table)
        return
