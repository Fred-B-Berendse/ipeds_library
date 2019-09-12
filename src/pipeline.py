from imputationtypes import ImputationTypes as it
from ipedscollection import IpedsCollection
from ipedsdatabase import IpedsDatabase

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

# ADM2017 table
adm_keep = ['unitid','admcon1','admcon2','admcon3','admcon4','admcon5','admcon6',
            'admcon7','admcon8','admcon9','applcn','applcnm','applcnw','admssn',
            'enrlt','enrlft','enrltpt','satvr25','satvr75','satmt25','satmt75',
            'acten25','acten75','actmt25','actmt75']
tc.update_meta(
        'adm2017',
        filepath='data/adm2017.csv',
        keep_columns=adm_keep,
        exclude_imputations=exclude_list)

# C2017 tables
c_keep = ['unitid','cipcode','awlevel','caiant','casiat','cbkaat','chispt','cnhpit',
          'cwhitt','c2mort']
tc.update_meta(
        'c2017_a',
        filepath='data/c2017_a.csv',
        keep_columns=c_keep,
        exclude_imputations=exclude_list)
tc.update_meta(
        'c2017_b',
        filepath='data/c2017_b.csv',
        keep_columns=c_keep,
        exclude_imputations=exclude_list)
tc.update_meta(
        'c2017_c',
        filepath='data/c2017_c.csv',
        keep_columns=c_keep,
        exclude_imputations=exclude_list)

# GR2017 table
gr_keep = ['unitid','chrtstat','cohort','graiant','grasiat','grbkaat','grhispt','grnhpit',
           'grwhitt','gr2mort']
tc.update_meta(
        'gr2017',
        filepath='data/gr2017.csv',
        keep_columns=gr_keep,
        exclude_imputations=exclude_list)

# GR2017_PELL_SSL table
pell_ssl_keep = ['unitid','pgsrtype','pgadjct','pgcmtot','ssadjct','sscmtot','nradjct','nrcmtot']
tc.update_meta(
        'gr2017_pell_ssl',
        filepath='data/gr2017_pell_ssl.csv',
        keep_columns=pell_ssl_keep,
        exclude_imputations=exclude_list)

tc.clean_tables()
merged_table = tc.join_all()

merged_table.write_csv('data/data_2017.csv')

ipdb = IpedsDatabase('localhost','5435','postgres','ipeds')
ipdb.to_sql(merged_table,'data_2017')

sqlstr = "SELECT unitid,instnm,city,stabbr FROM data_2017 LIMIT 5;"
result_set = ipdb.execute(sqlstr)
for r in result_set:
    print(r)

ipdb.close()