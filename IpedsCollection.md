# IpedsCollection class
A collection of IPEDS tables to be used for merging into a single table or a database.


## Object Attributes

### meta
    A dictionary consisting of table names as keys and meta-data of the table as values. The meta-data include columns to be extracted from the table and imputations to be excluded from the table when the table is cleaned.
    
    Example: 
    
    { 'table1' : {'table' : <IpedsTable object>,
                'keep_columns' : ['col1','col3','col5'], 
                'col_levels' : ['col4'],
                'exclude_imputations' : ['A','B','D','H']}, 
      'table2' : {'filepath' : 'data/table2.csv',
                'keep_columns' : ['col5','col7'],
                'filter_values' : {'col6':[3,5], 'col8':['G']}
                'exclude_imputations' : []} }
    
### merged_table
    An IpedsTable object containing merged table data once a merging method (merge_table or merge_all) is called. 

## Methods

### \__init__(self)
    Instantiates the classs.

### update_meta(self,name,table=None,filepath=None,keep_columns=None,col_levels=None,filter_values=None,exclude_imputations=None)
    Updates the meta attribute for the given table.

### drop_meta(self,name)
    Drops the given table from the meta dictionary.

### import_all(self)
    Checks all items in the meta dictionary. When it finds a filepath key, it reads in that csv file and inserts an IpedsTable object into the meta dictionary. 

    Before calling import_all:

     { 'table1' : {'table' : <IpedsTable object>,
                'keep_columns' : ['col1','col3','col5'], 
                'exclude_imputations' : ['A','B','D','H']}, 
      'table2' : {'filepath' : 'data/table2.csv',
                'keep_columns' : ['col5','col7'],
                'exclude_imputations' : []} }

    After calling import_all:

     { 'table1' : {'table' : <IpedsTable object>,
                'keep_columns' : ['col1','col3','col5'], 
                'exclude_imputations' : ['A','B','D','H']}, 
      'table2' : {'table' : <IpedsTable object>,
                'keep_columns' : ['col5','col7'],
                'exclude_imputations' : []} }    

### import_table(self,name)
    Imports an individual IpedsTable into the meta dictionary.

### make_multicols_all(self)
    For each table, converts columns listed in the meta key 'col_levels' to levels in a column multiindex.

### make_multicols(self,name)
    For the named table, converts columns listed in the meta key 'col_levels' to levels in a column multiindex.

### clean_all(self,dropna=False)
    Calls clean_table for every item in the meta dictionary. The dropna keyword is passed to clean_table.

### clean_table(self,name,dropna=False)
    Calls IpedsTable.keep_columns() to keep the columns specified in the `keep_columns` item of the table's meta entry. It then calls IpedsTable.purge_imputations() to exclude the imputations specified in the `exclude_imputations` item of the table's meta entry. If dropna is true, it will then drop any rows with null values.

### filter_all(self)
    Filters all tables, keeping only those rows containing values specified in each table's 'filter_values' dictionary.

### filter_values(self,name)
    Filters the named table, keeping only those rows containing values specified in the table's 'filter_values' dictionary.  

### merge_table(self,name,how='inner',keep_table=True)
    Merges the table with the table merged_table attribute joined on the 'unitid' column of each table. If the merged_table is empty, this table is copied into merged_table. If keep_table is set to False, the table is dropped from the meta dictionary once it is merged.

### merge_all(self,how='inner',keep_table=True)
    Merges all of the tables in the meta attribute. The result becomes the merged_table attribute of the object.

### pipeline_all(self,dropna=False,how='inner',keep_table=True)
    Imports (import_table), cleans (clean_table), and merges (merge_table) each of the tables in the meta attribute.