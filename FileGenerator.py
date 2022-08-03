import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


class FileGenerator:

    def __init__(self, index):
        self.df = pd.DataFrame(index)

    def __df_transpose(self):
        df_transpose = self.df.T
        return df_transpose

    def insert2df(self, value):
        loc = len(self.df.columns)
        self.df.insert(loc=loc, column=str(loc), value=value, allow_duplicates=False)

    def toMirror(self, prtsn_cols=['series ID', 'year']):
        """
        The method 'toMirror' convert dataframe into parquet files which
        all values are from type str.
        :param prtsn_cols: type list. Default 'series ID' and 'year' columns.
        """
        df = self.__df_transpose()
        print("*************************************************")
        table = pa.Table.from_pandas(df.astype(str))
        print("=================================================")
        pq.write_to_dataset(table, root_path="output.parquet", partition_cols=prtsn_cols)

    def toRaw(self):
        """
        The method 'toRaw' convert Series into json file which all values are from type str.
        """
        df = self.__df_transpose()
        for _, row in df.iterrows():
            df1 = row.iloc[0:3]
            df1.astype(str).to_json(row['raw_file'] + '.json')
