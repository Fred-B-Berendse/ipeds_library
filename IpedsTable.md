# IpedsTable class
A table of data from the IPEDS database or a merge of multiple IPEDS database tables. Many of the methods 

## Object Attributes
### df
    A pandas DataFrame containing tabular data
### columns
    A list of columns in the DataFrame

## Methods 
### \__init__(self, df=None, filepath=None)
    Instantiates an object of the IpedsTable class. A pandas DataFrame or the path to a csv file is required.
### \__len__(self)
    Returns the length of the DataFrame
### info(self,**kwargs)
    Returns Pandas DataFrame info. `kwargs` are  `DataFrame.info()` keywords.
### describe(self, **kwargs)
    Returns Pandas Dataframe statistics table. `kwargs` are  `DataFrame.describe()` keywords.
### head(self,n=5)
    Returns the first n rows of the DataFrame
### tail(self,n=5)
    Returns the last n rows of the Dataframe
### update_columns(self)
    Updates `self.df.columns` and `self.columns` with trimmed and lowercase column names
### rename_columns(self, name_dict)
    Renames columns using a name dictionary. Keys of the name dictionary contain old column names. Values of the dictionary are the new column names.
### keep_columns(self, column_list)
    Drops from the DataFrame columns in the list `column_list`
### drop_columns(self, column_list)
    Drops from the DataFrame columns in the list `column_list`
### extend_column_list(self, column_list='all')
    Returns an extended column list with the columns in `column list` and their correponding imputation columns If `column_list` is `'all'`, then the method returns all the columns in the DataFrame.
### get_imputation_columns(self, column_list='all')
    Returns a list of imputation columns corresponding to each of the columns in column_list. If `column_list` is `'all'`, then the method returns all the imputation columns in the DataFrame.
### dropna(self, column_list, how='all')
    Drops rows containing null values in any or all of the columns in `column_list`.
### purge_imputations(self, imputation_types, column_list='all', how='all')
    Drops rows where any or all of the columns in `column_list` contain any of the values in the list `imputation_types`. If `column_list` is `all`, then all columns in the DataFrame are used.
### write_csv(self, filepath)
    Writes the DataFrame to a comma-delimited csv file.
### make_multicols(self, col_levels)
    Converts the columns in col_levels to a level in the column multiindex. One can use this to convert a many-to-one table to a one-to-one table.
### filter_multicols(self,colval_dict)
    Filters the table's DataFrame to contain only those columns specified in the column-value dictionary. Keys in this dictionary are level names. Values in this dictionary are values within that level.
### filter_values(self,colval_dict)
    Filters the table's DataFrame to contain only rows whose values are specified in the colval dictionary. Keys in this dictionary are column names. Values of the dictionary are values in that column to be kept. 

