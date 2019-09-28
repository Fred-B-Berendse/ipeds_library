from imputationtypes import ImputationTypes as it
from ipedscollection import IpedsCollection
from ipedsdatabase import IpedsDatabase
from ipedstable import IpedsTable
import os
import numpy as np

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
pell_ssl_keep = ['unitid', 'psgrtype', 'pgadjct', 'pgcmtot', 'ssadjct',
                 'sscmtot', 'nradjct', 'nrcmtot']   
grp_filter_values = {'psgrtype':[2]}
tc.update_meta('gr2017_pell_ssl',
               filepath='data/gr2017_pell_ssl.csv',
               keep_columns=pell_ssl_keep,
               filter_values=grp_filter_values,
               exclude_imputations=exclude_list)

# Add all tables to the tc
tc.pipeline_all(keep_table=True, dropna=True)

# print("writing to file")
# tc.merged_table.write_csv('data/ipeds_2017.csv')

# print("Connecting to database")
# ipdb = IpedsDatabase('localhost', '5435', 'postgres', 'ipeds')
# ipdb = IpedsDatabase('database-1.cwtci684dbqt.us-west-1.rds.amazonaws.com',
#                      '5432',
#                      os.environ.get('AWS_RDS_USERNAME'),
#                      os.environ.get('AWS_RDS_PASSWORD'),
#                      'ipeds')
# print("Writing to table data_2017")
# ipdb.to_sql(tc.merged_table, 'data_2017')
# print("DONE!")
# ipdb.close()

# print("Reading from database")
# ipdb = IpedsDatabase('localhost', '5435', 'postgres', 'ipeds')
# ipdb = IpedsDatabase('database-1.cwtci684dbqt.us-west-1.rds.amazonaws.com',
#                      '5432',
#                      os.environ.get('AWS_RDS_USERNAME'),
#                      os.environ.get('AWS_RDS_PASSWORD'),
#                      'ipeds')
# data_2017 = ipdb.from_sql('data_2017')
# ipdb.close()

# get individual tables
hd = tc.meta['hd2017']['table']
adm = tc.meta['adm2017']['table']
c = tc.meta['c2017_c']['table']
gr = tc.meta['gr2017']['table']
grp = tc.meta['gr2017_pell_ssl']['table']

c_awlevelc = { 1 : "Award < 1 academic year",
               2 : "Award 1-4 academic years",
               3 : "Associate's degree",
               5 : "Bachelor's degree",
               7 : "Master's degree", 
               9 : "Doctor's degree", 
              10 : "Postbaccalaureate or Post-master's certificate"
             }

gr_cohort = { 1	: "Bachelor's equiv + other 2011 subcohorts (4-yr institution)",
              2	: "Bachelor's or equiv 2011 subcohort (4-yr institution)", 
              3	: "Other degree/certif-seeking 2011 subcohort (4-yr institution)", 
              4	: "Degree/certif-seeking students 2014 cohort ( 2-yr institution)"
            }

gr_chrtstat = { 10 : "Revised cohort",
                11 : "Exclusions", 
                12 : "Revised cohort minus exclusions",
                13 : "Completers: 150% time",
                14 : "Completers: programs <2 years (<=150% time)",
                15 : "Completers: programs 2-4 years (<=150% time)",
                16 : "Completers: bachelor's or equiv (<=150% time)",
                17 : "Completers: bachelor's or equiv (<=4 yrs)",
                18 : "Completers: bachelor's or equiv (5 yrs)",
                19 : "Completers: bachelor's or equiv (6 yrs)",
                20 : "Transfer-out students",
                22 : "Completers: <=100% time total",
                23 : "Completers: programs <2 yrs (100% time) (no racial/gender data)",
                24 : "Completers: programs 2-4 yrs (100% time) (no racial/gender data)",
                31 : "Noncompleters, still enrolled",
                32 : "Noncompleters, no longer enrolled"
              }

grp_psgrtype = { 1 : "Total 2011 cohort (4-year institution)",
                 2 : "Bachelor's degree 2011 cohort (4-year institution)",
                 3 : "Other degree/certificate 2011 cohort (4-year institution)",
                 4 : "Degree/certificate seeking 2014 cohort (<4-year institution)"
               }

# Perform a sums check on the graduation rate data
gr.df.fillna(0, inplace=True)
l2cols = ['gr2mort','graiant','grasiat','grbkaat','grhispt','grnhpit','grwhitt']
for c in l2cols:

    gr.df.loc[:,('12_check',c)] = gr.df.loc[:,(16,c)] +\
                                  gr.df.loc[:,(20,c)] +\
                                  gr.df.loc[:,(31,c)] +\
                                  gr.df.loc[:,(32,c)]

    gr.df.loc[:,('16_check',c)] = gr.df.loc[:,(17,c)] +\
                                  gr.df.loc[:,(18,c)] +\
                                  gr.df.loc[:,(19,c)]

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

