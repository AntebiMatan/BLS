# -------------------------------------------------------
#                       Matan Antebi
#                  Cell Phone: +9725229218
#         Email Address: matanantebi@mail.tau.ac.il
#                   Copyrights Reserved ©
# -------------------------------------------------------
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
from BLSAPI import *
from FileGenerator import *


def nonseasonal(series):
    """
    The function 'nonseasonal' calculate the unemployment rate over a year.
    :param series: type dictionary.
    :return: float type with precision 2.
    """
    temp = pd.DataFrame(series['data'])
    value = pd.to_numeric(temp['value'], errors='coerce').mean().round(decimals=2)
    return value


def dataframe2json(df):
    """
    The function 'dataframe2json' convert Series into json file which
    all values are from type str.
    :param df: series.
    """
    df1 = df.iloc[0:3]
    df1.astype(str).to_json(df['raw_file'] + '.json')


def dataframe2parquet(df, prtsn_cols=['series ID', 'year']):
    """
    The function 'dataframe2parquet' convert dataframe into parquet files which
    all values are from type str.
    :param df: dataframe.
    :param prtsn_cols: type list. Default 'series ID' and 'year' columns.
    """
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(table, root_path="output.parquet", partition_cols=prtsn_cols)


def time_array(params, delta=1):
    """
    The function 'time_array' takes the start-year and end-year and creates a time array.
    :param params: type of argeparse = {Namespace}.
    :param delta: type int. The resolution for steps in the time array. Defualt delta equals to 1.
    :return: years: type of list. Years in resolution of 1 year. Expect delta to be natural numer.
    """
    if type(delta) != int:
        raise Exception(f"'time_array' function expects delta argument to be natural number (integer and positive).")
    years = [*range(int(params.startyear), int(params.endyear) + 1, delta)]
    return years


def isTimeSeries(startyear, endyear):
    """
    The function 'isTimeSeries' checks if the arguments for startyear and endyear are valid in the sense of time series.
    :param startyear: type of str.
    :param endyear: type of str.
    """
    if not int(startyear) <= int(endyear):
        raise Exception(f"The 'startyear' and 'endyear' are not suitable for time series arguments")


def validate_args(params):
    """
    The function 'validate_args' checks if the arguments are valid.
    :param params: type of argparse = {Namespace}.
    :return: type of argparse = {Namespace}. The different is that the series ID's are unique (in case the weren't).
    """
    params.seriesIDs = list(set(params.seriesIDs.split(',')))  # unique(series ID's values).
    if not params.startyear.isnumeric() or not params.endyear.isnumeric():
        raise Exception(f"'startyear' or 'endyear' or both are not numeric (integers only):\n"
                        f"yearstart = {params.startyear}\n"
                        f" endyeat = {params.startyear}")
    isTimeSeries(params.startyear, params.endyear)
    return params


def main(params):
    blsapi = BLSAPI()
    filegen = FileGenerator(index=['series ID', 'year', 'value', 'received_date', 'raw_file'])
    years = time_array(params)  # define time array in terms of years.
    # df = pd.DataFrame(index=['series ID', 'year', 'value', 'received_date', 'raw_file'])
    for year in years:
        p = blsapi.post_request({"seriesid": params.seriesIDs,
                                 "startyear": year,
                                 "endyear": year})
        # headers = {'Content-type': 'application/json'}
        #
        # data = json.dumps({"seriesid": params.seriesIDs,
        #                    "startyear": year,
        #                    "endyear": year})
        # p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
        rcvdate = datetime.datetime.now().strftime("%d/%m/%y")
        json_data = json.loads(p.text)
        for series in json_data['Results']['series']:
            seriesId = series['seriesID']
            value = nonseasonal(series)
            rcrdname = f"{series['seriesID']}_{year}"
            filegen.insert2df([seriesId, year, value, rcvdate, rcrdname])
            # loc = len(df.columns)
            # df.insert(loc=loc, column=str(loc), value=[seriesId, year, value, rcvdate, rcrdname], allow_duplicates=False)
    filegen.toMirror()
    filegen.toRaw()

    # df_transpose = df.T
    # dataframe2parquet(df_transpose)
    # for _, row in df_transpose.iterrows():
    #     dataframe2json(row)
    print("finished")




