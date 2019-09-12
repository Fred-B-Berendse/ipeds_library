from imputationtypes import ImputationTypes as it
from ipedscollection import IpedsCollection
from dbpipeline import DatabasePipeline

# exclude_list = [
#         it.data_not_usable,
#         it.do_not_know,
#         it.left_blank,
#         it.not_applicable
#         ]

# tc = IpedsCollection()
# tc.update_meta(
#         'hd2017',
#         filepath='data/hd2017.csv',
#         keep_columns=['instnm','city','stabbr'],
#         exclude_imputations=exclude_list)
# tc.update_meta(
#         'adm2017',
#         filepath='data/adm2017.csv',
#         keep_columns='all',
#         exclude_imputations=exclude_list)
# tc.update_meta(
#         'gr2017',
#         filepath='data/gr2017.csv',
#         keep_columns='all',
#         exclude_imputations=exclude_list)

# tc.clean_tables()
# merged_table = tc.join_all()

print("attempting connection")
dp = DatabasePipeline('ipeds','postgres','localhost','5435')
print("Created DB Pipeline Object")
dp.add_step("SELECT datname FROM pg_database where datistemplate=false;")
dp.execute()
print("Executed check")
print(f'Row count: {dp.c.rowcount}')
for row in dp.c:
    print(row)
dp.close()

# cur.execute(sql.SQL("CREATE DATABASE {}").format(
#         sql.Identifier(self.db_name))

# SELECT table_schema,table_name
# FROM information_schema.tables
# ORDER BY table_schema,table_name;