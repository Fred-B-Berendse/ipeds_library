from imputationtypes import ImputationTypes as it
from cohorttypes import CohortTypes as ct
from ipedscollection import IpedsCollection
from ipedsdatabase import IpedsDatabase
from ipedstable import IpedsTable
import os
import numpy as np
import matplotlib.pyplot as plt 
from matplotlib import cm
import seaborn as sns
from copy import copy

exclude_list = [
        it.data_not_usable,
        it.do_not_know,
        it.left_blank,
        it.not_applicable
        ]

tc = IpedsCollection()

# HD2017 table
hd_keep = ['unitid', 'instnm', 'city', 'stabbr', 'iclevel', 'control',
           'hloffer', 'hbcu', 'tribal', 'locale', 'instsize', 'longitud',
           'latitude']
tc.update_meta('hd2017',
               filepath='data/hd2017.csv',
               keep_columns=hd_keep,
               exclude_imputations=exclude_list)

# ADM2017 table
adm_keep = ['unitid', 'admcon1', 'admcon2', 'admcon3', 'admcon4', 'admcon5',
            'admcon6', 'admcon7', 'admcon8', 'admcon9', 'applcn', 'applcnm',
            'applcnw', 'admssn', 'enrlt', 'enrlft', 'enrlpt', 'satvr25',
            'satvr75', 'satmt25', 'satmt75', 'acten25', 'acten75', 'actmt25',
            'actmt75']
tc.update_meta('adm2017',
               filepath='data/adm2017.csv',
               keep_columns=adm_keep,
               exclude_imputations=exclude_list)

# C2017 tables
c_keep = ['unitid', 'awlevelc', 'cstotlt', 'csaiant', 'csasiat', 'csbkaat',
          'cshispt', 'csnhpit', 'cswhitt', 'cs2mort']
c_filter_values = {'awlevelc':[5]}
tc.update_meta('c2017_c',
               filepath='data/c2017_c.csv',
               keep_columns=c_keep,
               filter_values=c_filter_values,
               exclude_imputations=exclude_list)

# GR2017 table
gr_keep = ['unitid', 'chrtstat', 'cohort', 'graiant', 'grasiat', 
           'grbkaat', 'grhispt', 'grnhpit', 'grwhitt', 'gr2mort']
gr_col_levels = ['chrtstat']
gr_filter_values = {'chrtstat':[12,16,17,18,19,20,31,32],'cohort':[2]}
tc.update_meta('gr2017',
               filepath='data/gr2017.csv',
               keep_columns=gr_keep,
               col_levels=gr_col_levels,
               filter_values=gr_filter_values,
               exclude_imputations=exclude_list)

# GR2017_PELL_SSL table
pell_ssl_keep = ['unitid', 'psgrtype', 'pgadjct', 'pgcmbac', 'ssadjct',
                 'sscmbac', 'nradjct', 'nrcmbac']   
grp_filter_values = {'psgrtype':[2]}
tc.update_meta('gr2017_pell_ssl',
               filepath='data/gr2017_pell_ssl.csv',
               keep_columns=pell_ssl_keep,
               filter_values=grp_filter_values,
               exclude_imputations=exclude_list)

# Process all of the tables, but do not merge
tc.import_all()
tc.clean_all()
tc.filter_all()
tc.make_multicols_all()

# get the completion rate table
c = tc.meta['c2017_c']['table']

# For some reason, the 'cstotlt' column is not equal to the sum of the races
race_cols = ['csaiant', 'csasiat', 'csbkaat', 
             'cshispt', 'csnhpit', 'cswhitt', 'cs2mort']
c.df['cstotlt_adj']=c.df[race_cols].sum(axis=1)

# Create percentage columns for each racial type
for rc in race_cols:
    pct_col = rc + '_pct'
    c.df[pct_col] = c.df[rc]/c.df['cstotlt_adj']*100

# get the graduation rate table
gr = tc.meta['gr2017']['table']

# Perform a sums check on the graduation rate data
gr.df.fillna(0, inplace=True)
l2cols = ['gr2mort','graiant','grasiat','grbkaat','grhispt','grnhpit','grwhitt']
for col in l2cols:

    gr.df.loc[:,('12_check',col)] = gr.df.loc[:,(16,col)] +\
                                  gr.df.loc[:,(20,col)] +\
                                  gr.df.loc[:,(31,col)] +\
                                  gr.df.loc[:,(32,col)]

    gr.df.loc[:,('16_check',col)] = gr.df.loc[:,(17,col)] +\
                                  gr.df.loc[:,(18,col)] +\
                                  gr.df.loc[:,(19,col)]

col12_cohort = gr.df[(12,'cohort')].copy()
col16_cohort = gr.df[(16,'cohort')].copy()
gr.df.drop([(12,'cohort'),(16,'cohort')],axis=1,inplace=True)
mask_12_check = gr.df.apply(lambda r: np.all(r[12]==r['12_check']),axis=1)
gr.df = gr.df[mask_12_check]
mask_16_check = gr.df.apply(lambda r: np.all(r[16]==r['16_check']),axis=1)
gr.df = gr.df[mask_16_check]
gr.df[(12,'cohort')] = col12_cohort
gr.df[(16,'cohort')] = col16_cohort 
gr.df.sort_index(level=0, axis=1, inplace=True)
gr.df.drop(['12_check','16_check'],axis=1,inplace=True)

# Get the pell graduation rate table
grp = tc.meta['gr2017_pell_ssl']['table']

# Create percentage columns for each recipient type
recipient_cnts = ['pgadjct','ssadjct','nradjct']
recipient_comps = ['pgcmbac','sscmbac','nrcmbac']

for rcnt,rcmp in zip(recipient_cnts,recipient_comps):
    pct_comp = rcmp + '_pct'
    grp.df[pct_comp] = grp.df[rcmp]/grp.df[rcnt]*100

# Drop any rows without a valid completion percentage
grp.df.dropna(axis=0, how='any', inplace=True)

# Merge all tables
tc.merge_all()

# Write merged table to CSV file
print("writing to file")
tc.merged_table.write_csv('data/ipeds_2017.csv')

print("Connecting to database")
# ipdb = IpedsDatabase('localhost', '5435', 'postgres', 'ipeds')
ipdb = IpedsDatabase('database-1.cwtci684dbqt.us-west-1.rds.amazonaws.com',
                     '5432',
                     os.environ.get('AWS_RDS_USERNAME'),
                     os.environ.get('AWS_RDS_PASSWORD'),
                     'ipeds')
print("Writing to table data_2017")
ipdb.to_sql(tc.merged_table, 'data_2017')
print("DONE!")
ipdb.close()

# print("Reading from database")
# ipdb = IpedsDatabase('localhost', '5435', 'postgres', 'ipeds')
# ipdb = IpedsDatabase('database-1.cwtci684dbqt.us-west-1.rds.amazonaws.com',
#                      '5432',
#                      os.environ.get('AWS_RDS_USERNAME'),
#                      os.environ.get('AWS_RDS_PASSWORD'),
#                      'ipeds')
# data_2017 = ipdb.from_sql('data_2017')
# ipdb.close()
