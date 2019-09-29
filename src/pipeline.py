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

# get the pell graduation rate table
grp = tc.meta['gr2017_pell_ssl']['table']

# Create percentage columns for each recipient type
recipient_cnts = ['pgadjct','ssadjct','nradjct']
recipient_comps = ['pgcmbac','sscmbac','nrcmbac']

for rcnt,rcmp in zip(recipient_cnts,recipient_comps):
    pct_comp = rcmp + '_pct'
    grp.df[pct_comp] = grp.df[rcmp]/grp.df[rcnt]*100

# Drop any rows without a valid completion percentage
grp.df.dropna(axis=0, how='any', inplace=True)

# tc.merge_all()
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

#### EXPLORATORY DATA ANALYSIS ####
hd = tc.meta['hd2017']['table']
adm = tc.meta['adm2017']['table']
c = tc.meta['c2017_c']['table']
gr = tc.meta['gr2017']['table']
grp = tc.meta['gr2017_pell_ssl']['table']

# merge each dataframe with hd information
for t in [adm,c,gr,grp]:
    t.df = t.df.merge(hd.df,how='left',on='unitid')
    t.columns = t.df.columns

# ACT/SAT plots for hbcu vs non-hbcu colleges
fig, ax = plt.subplots(2,4,figsize=(12,8), gridspec_kw={'wspace':0.5})
stat_cols = ['satvr25','satvr75','satmt25','satmt75','acten25','acten75','actmt25','actmt75']
titles = ['SAT Verbal 25th Percentile',
          'SAT Verbal 75th Percentile',
          'SAT Math 25th Percentile',
          'SAT Math 75th Percentile',
          'ACT English 25th Percentile',
          'ACT English 75th Percentile',
          'ACT Math 25th Percentile',
          'ACT Math 75th Percentile'
         ]

for i,stat in enumerate(stat_cols):
    axi = ax[i//4,i%4]
    sns.boxplot(x='hbcu', y=stat, data=adm.df, whis='range', ax=axi)
    axi.set_ylabel(titles[i])
    axi.set_xticklabels(['Yes','No'])
    axi.set_xlabel('HBCU')

# ACT/SAT plots for tribal vs non-tribal colleges
fig, ax = plt.subplots(2,4,figsize=(12,8),gridspec_kw={'wspace':0.5})
stat_cols = ['satvr25','satvr75','satmt25','satmt75','acten25','acten75','actmt25','actmt75']
titles = ['SAT Verbal 25th Percentile',
          'SAT Verbal 75th Percentile',
          'SAT Math 25th Percentile',
          'SAT Math 75th Percentile',
          'ACT English 25th Percentile',
          'ACT English 75th Percentile',
          'ACT Math 25th Percentile',
          'ACT Math 75th Percentile'
         ]
for i,stat in enumerate(stat_cols):
    axi = ax[i//4,i%4]
    sns.boxplot(x='tribal', y=stat, whis='range', data=adm.df, ax=axi)
    axi.set_ylabel(titles[i])
    axi.set_xticklabels(['Yes','No'])
    axi.set_xlabel('Tribal')

# ACT/SAT plots by locale
fig, ax = plt.subplots(2,4,figsize=(12,8),gridspec_kw={'wspace':0.5, 'hspace':0.6})
stat_cols = ['satvr25','satvr75','satmt25','satmt75','acten25','acten75','actmt25','actmt75']
titles = ['SAT Verbal 25th Percentile',
          'SAT Verbal 75th Percentile',
          'SAT Math 25th Percentile',
          'SAT Math 75th Percentile',
          'ACT English 25th Percentile',
          'ACT English 75th Percentile',
          'ACT Math 25th Percentile',
          'ACT Math 75th Percentile'
         ]
xticks = ['City: Lg', 'City: Md', 'City: Sm',
          'Suburb: Lg', 'Suburb: Md', 'Suburb: Sm',
          'Town: Fringe', 'Town: Distant', 'Town: Remote',
          'Rural: Fringe', 'Rural: Distant', 'Rural: Remote']
for i,stat in enumerate(stat_cols):
    axi = ax[i//4,i%4]
    sns.boxplot(x='locale', y=stat, whis='range', data=adm.df, ax=axi)
    axi.set_ylabel(titles[i])
    axi.set_xticklabels(xticks,rotation=90)
    axi.set_xlabel('Locale')

# ACT/SAT plots by institution size
fig, ax = plt.subplots(2,4,figsize=(12,8),gridspec_kw={'wspace':0.5, 'hspace':0.6})
stat_cols = ['satvr25','satvr75','satmt25','satmt75','acten25','acten75','actmt25','actmt75']
titles = ['SAT Verbal 25th Percentile',
          'SAT Verbal 75th Percentile',
          'SAT Math 25th Percentile',
          'SAT Math 75th Percentile',
          'ACT English 25th Percentile',
          'ACT English 75th Percentile',
          'ACT Math 25th Percentile',
          'ACT Math 75th Percentile'
         ]
xticks = ['under 1000', '1000-4999', '5000-9999', '10000-19999', 'over 20000']
for i,stat in enumerate(stat_cols):
    axi = ax[i//4,i%4]
    sns.boxplot(x='instsize', y=stat, whis='range', data=adm.df, ax=axi)
    axi.set_ylabel(titles[i])
    axi.set_xticklabels(xticks,rotation=90)
    axi.set_xlabel('Institution Size')

# Overall Completion rates by race
fig, ax = plt.subplots(1,1,figsize=(10,6))
cnt_categories = ['cswhitt', 'cshispt', 'csbkaat',
                  'csasiat', 'csaiant', 'csnhpit',
                  'cs2mort']
pct_categories = ['cswhitt_pct', 'cshispt_pct', 'csbkaat_pct',
                  'csasiat_pct', 'csaiant_pct', 'csnhpit_pct',
                  'cs2mort_pct']
labels = ['white', 'hispanic', 'black', 
          'asian', 'american indian', 'pacific islander',
          'two or more races']
xlabels = ["Bachelor's Completions","US Census (2018 est.)"]
x = range(len(xlabels))
width = 0.8
colors = cm.tab10(np.linspace(0,1,len(cnt_categories)))

## Completion counts
prev_cc = 0

for i,cc in enumerate(cnt_categories):
    compl = c.df[cc].sum()
    ax.bar(x[0], compl, 
           width=width, 
           bottom=prev_cc, 
           label=labels[i], 
           color=colors[i])
    prev_cc += compl

ax.set_xticks(x)
ax.set_xticklabels(xlabels)
ax.set_ylabel('Number of students')

## Completion pct
total = prev_cc
prev_cc = 0
ax2 = ax.twinx()
for i,cc in enumerate(cnt_categories):
    compl = c.df[cc].sum()/total*100
    ax2.bar(x[0], compl, 
            width=width, 
            bottom=prev_cc, 
            label=labels[i], 
            color=colors[i])
    prev_cc += compl
ax2.set_ylabel('Percent')

## US Population
census_pct = [60.4,18.3,13.4,5.9,1.3,0.2,2.7]
prev_pct = 0
for i,cc in enumerate(cnt_categories):
    ax2.bar(x[1], census_pct[i], 
            width=width, 
            bottom=prev_pct, 
            color=colors[i])
    prev_pct += census_pct[i]
ax2.set_ylabel('Percent')

ax2.set_xlim(-0.5,2.2)
ax2.legend(loc='best')

# Distribution of completion rate percentage for each race 
pct_categories = ['cswhitt_pct', 'cshispt_pct', 'csbkaat_pct',
                  'csasiat_pct', 'csaiant_pct', 'csnhpit_pct',
                  'cs2mort_pct']

labels = ['white', 'hispanic', 'black', 
          'asian', 'am. indian', 'pac. isl.',
          '2+ races']

fig, ax = plt.subplots(1,1,figsize=(10,6))
dataset = []
for pc in pct_categories:
    dataset.append(c.df.loc[:,pc].dropna().values)

parts = ax.violinplot(positions=range(len(labels)), 
                      dataset=dataset,
                      showextrema=False
                     )

for i,pc in enumerate(parts['bodies']):
    pc.set_facecolor(colors[i])
    pc.set_edgecolor('black')

ax.set_xticks(range(len(labels)))
ax.set_xticklabels(labels)
ax.set_ylabel('Percentage of Completions')
ax.set_title("Institution-wide Completion Percentage by Race")

# Distribution of completion rate percentage for Pell, SSL, and non-SSL
pct_categories = ['pgcmbac_pct', 'sscmbac_pct', 'nrcmbac_pct']

labels = ['Pell Grant', 'SSL', 'Non-Recipient']

fig, ax = plt.subplots(1,1,figsize=(10,6))
dataset = []
for pc in pct_categories:
    dataset.append(grp.df.loc[:,pc].dropna().values)

parts = ax.violinplot(positions=range(len(labels)), 
                      dataset=dataset,
                      showextrema=False
                     )

for i,pc in enumerate(parts['bodies']):
    pc.set_facecolor(colors[i])
    pc.set_edgecolor('black')

ax.set_xticks(range(len(labels)))
ax.set_xticklabels(labels)
ax.set_ylabel('Percentage of Completions')
ax.set_title("Institution-wide Completion Percentage by Recipient Status")

# Scatterplot of completion percentages
fig, ax = plt.subplots(1,1,figsize=(6,6))
ax.scatter(grp.df['nrcmbac_pct'],
           grp.df['pgcmbac_pct'],
           color=colors[0],
           label=labels[0],
           alpha=0.3)
ax.scatter(grp.df['nrcmbac_pct'],
           grp.df['sscmbac_pct'],
           color=colors[1],
           label=labels[1],
           alpha=0.3)
ax.set_ylabel('Completion Pct for Recipients')
ax.set_xlabel('Completion Pct for Non-recipients')
ax.set_title('Recipients vs. Non-Recipients by Institution')
ax.legend(loc='best')