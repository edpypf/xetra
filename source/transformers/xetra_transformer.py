"""Xetra ETL Component"""
from typing import NamedTuple
from source.common.s3 import *

class XetraSourceConfig(NamedTuple):
    """
    Class for source configuration data
    
    src_first_extract_date: determines the date for extracting the source
    src_columns: souce column names
    src_col_date: column name for date in source
    src_col_isin: column name for time in source
    src_col_time: column name for time in source
    src_col_start_price: column name for starting price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col_traded_vol: column name for traded volume in source
    """
    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str

class XetraETL():
    """
    Reads the Xetra data, transforms and writes the transformed to target
    """

    def __init__(self, s3_bucket_src: S3BucketConnector,
                 s3_bucket_trg: S3BucketConnector, meta_key: str,
                 src_args: XetraSourceConfig, trg_args: XetraSourceConfig):
        """Constructor for XetraTransformer
        :param s3_bucket_src: connection to source s3 bucket
        :param s3_ubcket_trg: connection to target s3 bucket
        :param meta_key: used as self.meta_key -> key of meta file
        :param src_args: NamedTouple class with source configuration data
        :param trg_args: NamedTouple class with target configuration data
        """
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args
        self.extract_date = 
        self.extract_date_list = 
        self.meta_update_list = 

    def extract(self, bucket, date_list):
        files = [key for date in date_list for key in list_files_in_prefix(bucket, date)]
        df=pd.concat([read_csv_to_df(bucket, file) for file in files], ignore_index=True)
        return df

    def transform_report1(self, df, columns, arg_date):
        df=df.loc[:, columns]
        df.dropna(inplace=True)
        df['opening_price'] = df.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('first')
        df['closing_price'] = df.sort_values(by=['Time']).groupby(['ISIN', 'Date'])['StartPrice'].transform('last')
        df = df.groupby(['ISIN', 'Date'], as_index=False).agg(opening_price_eur=('opening_price', 'min'), closing_price_eur=('closing_price', 'min'), minimum_price_eur=('MinPrice', 'min'), maximum_price_eur=('MaxPrice', 'max'), daily_traded_volume=('TradedVolume', 'sum'))
        df['prev_closing_price'] = df.sort_values(by=['Date']).groupby(['ISIN'])['closing_price_eur'].shift(1)
        df['change_prev_closing_%']=(df['closing_price_eur']-df['prev_closing_price'])/df['prev_closing_price']*100    
        df.drop(columns=['prev_closing_price'], inplace=True)
        df=df.round(decimals=2)
        df=df[df.Date >= arg_date]
        return df

    def load(self, bucket, df, trg_key, trg_format, meta_key, extract_date_list):
        key = trg_key + datetime.today().strftime("%Y%m%d_%H%M%S") + trg_format
        write_df_to_s3_parquet(bucket, df, key)
        update_meta_file(bucket, meta_key, extract_date_list, src_format)
        return True

    def etl_report1(self, bucket_src, bucket_trg, date_list, columns, arg_date, trg_key, trg_format):
        df=self.extract(bucket_src, date_list)
        df=self.transform_report1(df, columns, arg_date)
        extract_date_list = [date for date in date_list if date > arg_date]
        self.load(bucket_trg, df, trg_key, trg_format, meta_key, extract_date_list)
        return True






















