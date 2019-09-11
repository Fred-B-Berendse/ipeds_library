from imputationtypes import ImputationTypes
from ipedstable import IpedsTable

class IpedsCollection(object):
    ''' A collection of multiple IpedsTables '''
    

    def __init__(self):
        self.meta = {}
        return

    def add_table(self,name,table):
        if not isinstance(table,IpedsTable):
            raise TypeError("table must be an instance of IpedsTable.")
        setattr(self,name,table)
        return

    def add_table_from_file(self,name,filepath):
        table = IpedsTable(filepath=filepath)
        setattr(self,name,table)
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
            entry.update({'table':table})
        
        if filepath:
            entry.update({'filepath':filepath})
                
        if keep_columns:
            entry.update({'keep_columns':keep_columns}) 
            
        if exclude_imputations:
            entry.update({'exclude_imputations':exclude_imputations})
        return

    def drop(self,name):
        delattr(self,name)
        del self.meta[name]
        return 

    def import_tables(self):
        #import tables in the meta that are not currently in the namesapce
        pass

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
        for i,name in enumerate(self.meta.keys()):
            table = getattr(self,name)
            if i == 0:
                merged_df = table.df
            else:
                merged_df = merged_df.join(table.df,rsuffix=name)

        return IpedsTable(df=merged_df)


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

    tc = IpedsCollection()
    tc.update_meta(
            'adm2017',
            table=adm2017,
            keep_columns='all',
            exclude_imputations=exclude_list)
    tc.update_meta(
            'hd2017',
            table=hd2017,
            keep_columns=['instnm','city','stabbr'],
            exclude_imputations=exclude_list)
    tc.update_meta(
            'gr2017',
            filepath='data/gr2017.csv',
            keep_columns='all',
            exclude_imputations=exclude_list)
   # merged_table = tc.join_all()