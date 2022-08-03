import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
import os


class FileGenerator:
    """
    The class 'FileGenerator' responsible for creating the suitable files (json/ parquet or both).
    """

    def __init__(self, index):
        self.rcvdate = datetime.datetime.now().strftime("%d/%m/%y")
        self.df = pd.DataFrame(index)

    def __df_transpose(self):  # Private method
        df_transpose = self.df.T
        print(self.df.T.iloc[0])
        df2 = df_transpose.rename(columns=df_transpose.iloc[0]).iloc[1:]
        return df2

    def insert2df(self, value):
        value.insert(3, self.rcvdate)
        loc = len(self.df.columns)
        self.df.insert(loc=loc, column=str(loc), value=value, allow_duplicates=False)

    def toMirror(self, prtsn_cols=['series ID', 'year']):
        """
        The method 'toMirror' convert dataframe into parquet files which
        all values are from type str.
        :param prtsn_cols: type list. Default 'series ID' and 'year' columns.
        """
        df = self.__df_transpose()
        table = pa.Table.from_pandas(df.astype(str))
        pq.write_to_dataset(table, root_path="output.parquet", partition_cols=prtsn_cols)

    def toRaw(self):
        """
        The method 'toRaw' convert Series into json file which all values are from type str.
        """
        if not os.path.exists("/output json"):
            os.makedirs("/output json")
        df = self.__df_transpose()
        for _, row in df.iterrows():
            df1 = row.iloc[0:3]
            df1.astype(str).to_json("output json/" + row['raw_file'] + '.json')
