from imputationtypes import ImputationTypes as it
from ipedscollection import IpedsCollection
from dbpipeline import DatabasePipeline as dp

if __name__ == '__main__':
    
    exclude_list = [
            it.data_not_usable,
            it.do_not_know,
            it.left_blank,
            it.not_applicable
            ]

    tc = IpedsCollection()
    tc.update_meta(
            'hd2017',
            filepath='data/hd2017.csv',
            keep_columns=['instnm','city','stabbr'],
            exclude_imputations=exclude_list)
    tc.update_meta(
            'adm2017',
            filepath='data/adm2017.csv',
            keep_columns='all',
            exclude_imputations=exclude_list)
    tc.update_meta(
            'gr2017',
            filepath='data/gr2017.csv',
            keep_columns='all',
            exclude_imputations=exclude_list)

    tc.clean_tables()
    merged_table = tc.join_all()