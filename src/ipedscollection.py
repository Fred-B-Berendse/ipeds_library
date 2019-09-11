from imputationtypes import ImputationTypes
from ipedstable import IpedsTable
from copy import copy

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
                entry.update({'table':copy(table)})
            else:
                raise TypeError('table must be an instance of IpedsTable')
        
        if filepath:
            entry.update({'filepath':filepath})
                
        if keep_columns:
            entry.update({'keep_columns':keep_columns}) 
            
        if exclude_imputations:
            entry.update({'exclude_imputations':exclude_imputations})
        return

    def drop(self,name):
        del self.meta[name]
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
        self.validate_table_imports()
        merged_df = None
        for name,entry in self.meta.items():
            table = entry['table']
            if not merged_df:
                merged_df = table.df
            else:
                merged_df = merged_df.join(table.df,on='unitid',rsuffix=name)
        return IpedsTable(df=merged_df)

    def validate_table_imports(self):
        if not all(['table' in entry.keys() for entry in self.meta.values()]):
            raise KeyError('One or more tables has not been imported.')
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
    #column_list = ['enrlpt']
    #adm2017.purge_imputations(exclude_list,column_list,how='any')
    #adm2017.write_csv('data/test.csv')

    tc = IpedsCollection()
    tc.update_meta(
            'hd2017',
            table=hd2017,
            keep_columns=['instnm','city','stabbr'],
            exclude_imputations=exclude_list)
    tc.update_meta(
            'adm2017',
            table=adm2017,
            keep_columns='all',
            exclude_imputations=exclude_list)
    tc.update_meta(
            'gr2017',
            filepath='data/gr2017.csv',
            keep_columns='all',
            exclude_imputations=exclude_list)

    tc.clean_tables()
   # merged_table = tc.join_all()