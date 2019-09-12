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
hd_keep = ['instnm','city','stabbr']

tc.update_meta(
        'hd2017',
        filepath='data/hd2017.csv',
        keep_columns=hd_keep,
        exclude_imputations=exclude_list)

# ADM2017 table
adm_keep = []
tc.update_meta(
        'adm2017',
        filepath='data/adm2017.csv',
        keep_columns=adm_keep,
        exclude_imputations=exclude_list)

# C2017 tables
c_keep = []
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
gr_keep = []
tc.update_meta(
        'gr2017',
        filepath='data/gr2017.csv',
        keep_columns=gr_keep,
        exclude_imputations=exclude_list)

# GR2017_PELL_SSL table
pell_ssl_keep = []
tc.update_meta(
        'gr2017_pell_ssl',
        filepath='data/gr2017_pell_ssl.csv',
        keep_columns=pell_ssl_keep,
        exclude_imputations=exclude_list)

tc.clean_tables()
merged_table = tc.join_all()

ipdb = IpedsDatabase('localhost','5435','postgres','ipeds')
ipdb.to_sql(merged_table,'data_2017')

sqlstr = "SELECT unitid,instnm,city,stabbr FROM data_2017 LIMIT 5;"
result_set = ipdb.execute(sqlstr)
for r in result_set:
    print(r)

ipdb.close()