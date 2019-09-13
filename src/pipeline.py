from imputationtypes import ImputationTypes as it
from ipedscollection import IpedsCollection
from ipedsdatabase import IpedsDatabase
from ipedstable import IpedsTable

import numpy as np

exclude_list = [
        it.data_not_usable,
        it.do_not_know,
        it.left_blank,
        it.not_applicable
        ]

tc = IpedsCollection()

# HD2017 table
hd_keep = ['unitid','instnm','city','stabbr','iclevel','control',
           'hloffer','hbcu','tribal','locale','instsize','longitud','latitude']
tc.update_meta(
        'hd2017',
        filepath='data/hd2017.csv',
        keep_columns=hd_keep,
        exclude_imputations=exclude_list)

# # ADM2017 table
# adm_keep = ['unitid','admcon1','admcon2','admcon3','admcon4','admcon5','admcon6',
#             'admcon7','admcon8','admcon9','applcn','applcnm','applcnw','admssn',
#             'enrlt','enrlft','enrlpt','satvr25','satvr75','satmt25','satmt75',
#             'acten25','acten75','actmt25','actmt75']
# tc.update_meta(
#         'adm2017',
#         filepath='data/adm2017.csv',
#         keep_columns=adm_keep,
#         exclude_imputations=exclude_list)

# # C2017 tables
# c_keep = ['unitid','cipcode','awlevel','caiant','casiat','cbkaat','chispt','cnhpit',
#           'cwhitt','c2mort']
# tc.update_meta(
#         'c2017_a',
#         filepath='data/c2017_a.csv',
#         keep_columns=c_keep,
#         exclude_imputations=exclude_list)

# GR2017 table
gr_keep = ['unitid','chrtstat','cohort','graiant','grasiat','grbkaat','grhispt','grnhpit',
           'grwhitt','gr2mort']
tc.update_meta(
        'gr2017',
        filepath='data/gr2017.csv',
        keep_columns=gr_keep,
        exclude_imputations=exclude_list)

# GR2017_PELL_SSL table
pell_ssl_keep = ['unitid','psgrtype','pgadjct','pgcmtot','ssadjct','sscmtot','nradjct','nrcmtot']
tc.update_meta(
        'gr2017_pell_ssl',
        filepath='data/gr2017_pell_ssl.csv',
        keep_columns=pell_ssl_keep,
        exclude_imputations=exclude_list)

# for t in ['hd2017','adm2017','c2017_a','gr2017','gr2017_pell_ssl']:
#         tc.import_table(t)
#         print(f'{t} before')
#         print(tc.meta[t]['table'].info())
#         tc.clean_table(t,dropna=True)
#         print(f'{t} after')
#         print(tc.meta[t]['table'].info())

tc.pipeline_all(keep_table=False)
# print("writing to file")
#tc.merged_table.write_csv('data/ipeds_2017.csv')
# print("writing to database")
# write_table = IpedsTable(df=tc.merged_table.df[0:1000])
# ipdb = IpedsDatabase('localhost','5435','postgres','ipeds')
# ipdb.to_sql(write_table,'data_2017')
# print("DONE!")
# ipdb.close()


