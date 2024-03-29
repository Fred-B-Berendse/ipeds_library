import pandas as pd
import csv


class IpedsTable(object):
    ''' An object containing data and methods from a single IPEDS data table.
    '''

    def __init__(self, df=None, filepath=None, data=None, columns=None):
        self.df = df
        self.columns = None

        if isinstance(df, pd.DataFrame):
            self.df = df
        elif filepath:
            self.df = pd.read_csv(filepath, encoding="cp1252")
        elif data:
            self.df = pd.DataFrame(data, columns=columns)
        else:
            self.df = pd.DataFrame([])
        self.update_columns()

    def __len__(self):
        return len(self.df)

    def info(self, **kwargs):
        return self.df.info(**kwargs)

    def describe(self, **kwargs):
        return self.df.describe(**kwargs)

    def head(self, n=5):
        return self.df.head(n)

    def tail(self, n=5):
        return self.df.tail(n)

    def update_columns(self):
        self.df.columns = [c.strip().lower() for c in self.df.columns]
        self.columns = self.df.columns

    def rename_columns(self, name_dict):
        self.df.rename(mapper=name_dict, axis=1, inplace=True)
        self.columns = self.df.columns

    def keep_columns(self, column_list):
        columns_to_drop = (self.columns).difference(column_list).values
        self.drop_columns(columns_to_drop)

    def drop_columns(self, column_list):
        self.df.drop(columns=column_list, inplace=True)
        self.columns = self.df.columns

    def extended_column_list(self, column_list='all'):
        if column_list == 'all':
            return self.columns
        else:
            return column_list + self.get_imputation_columns(column_list)

    def get_imputation_columns(self, column_list='all'):
        col_list = self.columns if column_list == 'all' else column_list
        imputation_columns = ['x'+c for c in col_list if 'x'+c in self.columns]
        return list(set(imputation_columns))

    def dropna(self, column_list, how='all'):
        if how.lower() not in ['all', 'any']:
            raise ValueError('Invalid method. Valid methods are "all"' +
                             'and "any".')
        self.df.dropna(axis=0, how=how, subset=column_list, inplace=True)

    def purge_imputations(self,
                          imputation_types,
                          column_list='all',
                          how='all'):
        if how.lower() not in ['all', 'any']:
            raise ValueError('Invalid method. Valid methods are "all"' +
                             'and "any".')

        imputation_columns = self.get_imputation_columns(column_list)
        data_slice = self.df.loc[:, imputation_columns]
        notin_imputation_list = data_slice.applymap(
                lambda v: v not in imputation_types)

        if how.lower() == 'all':
            row_mask = notin_imputation_list.all(axis=1)
        else:
            row_mask = notin_imputation_list.any(axis=1)

        self.df = self.df[row_mask]

    def write_csv(self, filepath):
        self.df.to_csv(filepath,
                       header=True,
                       quoting=csv.QUOTE_NONNUMERIC)

    def make_multicols(self, col_levels):
        self.df.set_index(col_levels, inplace=True)
        self.df = self.df.unstack()
        self.df = self.df.swaplevel(i=0, j=1, axis=1)
        self.df.sort_index(level=0, axis=1, inplace=True)
        self.df.reset_index(inplace=True)
        self.columns = self.df.columns
        
    def filter_multicols(self, colval_dict):
        col_mask = self.df.columns.levels[0] == 'unitid'
        for col, vals in colval_dict.items():
            level = self.df.columns.names.index(col)
            col_mask = col_mask | [c in vals for c in self.df.columns.levels[level]]
        cols = self.df.columns.levels[0][col_mask]
        self.df = self.df[cols]
        self.columns = self.df.columns
    
    def filter_values(self, colval_dict):
        for col, vals in colval_dict.items():
            if not isinstance(vals, (list, tuple)):
                vals = [vals]
            mask = [c in vals for c in self.df[col]]
            self.df = self.df[mask]

    def make_pct_column(self, count_col, total_col, replace=False, dropna=False):
        pct_col = count_col + '_pct'
        self.df[pct_col] = self.df[count_col]/self.df[total_col]*100
        if replace: 
            self.df.drop(count_col, axis=1, inplace=True)
        if dropna:
            self.df.dropna(axis=0, how='any')

    def make_pct_columns(self, count_cols, total_cols, replace=False, dropna=False):
        for part, total in zip(count_cols, total_cols):
            self.make_pct_column(part, total, replace=replace, dropna=dropna)
    
    def map_values(self, map_dict):
        for col, val_map in map_dict.items():
            for old_val, new_val in val_map.items():
                mask = self.df[col] == old_val
                self.df.loc[mask, col] = new_val

    def encode_column(self, column):
        cat_cnts = self.df[column].value_counts()
        categories = cat_cnts.index.values
        for c in categories[1:]:
            new_category = column + '_' + str(c)
            self.df[new_category] = (self.df[column] == c).astype(int)
        self.df.drop(column, axis=1, inplace=True)
        self.update_columns()

    def encode_columns(self, columns):
        for col in columns:
            self.encode_column(col)