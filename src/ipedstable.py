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
        return

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
        return

    def rename_columns(self, name_dict):
        self.df.rename(mapper=name_dict, axis=1, inplace=True)
        self.columns = self.df.columns
        return

    def keep_columns(self, column_list):
        columns_to_drop = (self.columns).difference(column_list).values
        self.drop_columns(columns_to_drop)
        return

    def drop_columns(self, column_list):
        self.df.drop(columns=column_list, inplace=True)
        self.columns = self.df.columns
        return

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
        return

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
        return

    def write_csv(self, filepath):
        self.df.to_csv(filepath,
                       header=True,
                       quoting=csv.QUOTE_NONNUMERIC)
        return

    def make_multicols(self, col_levels):
        self.df.set_index(col_levels, inplace=True)
        self.df = self.df.unstack()
        self.df = self.df.swaplevel(i=0, j=1, axis=1)
        self.df.sort_index(level=0, axis=1, inplace=True)
        self.df.reset_index(inplace=True)
        self.columns = self.df.columns
        return        
        
    def filter_multicols(self, colval_dict):
        col_mask = self.df.columns.levels[0] == 'unitid'
        for col, vals in colval_dict.items():
            level = self.df.columns.names.index(col)
            col_mask = col_mask | [c in vals for c in self.df.columns.levels[level]]
        cols = self.df.columns.levels[0][col_mask]
        self.df = self.df[cols]
        self.columns = self.df.columns
        return
    
    def filter_values(self, colval_dict):
        for col, vals in colval_dict.items():
            if not isinstance(vals, (list, tuple)):
                vals = list(vals)
            mask = [c in vals for c in self.df[col]]
            self.df = self.df[mask]
        return