"""Xetra ETL Component"""
from typing import NamedTuple
from source.common.s3 import s3BucketConnector

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

    def __init__(self, s3_bucket_src: s3BucketConnector,
                 s3_bucket_trg: s3BucketConnector, meta_key: str,
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

    def extract(self):
        pass

    def transform_report1(self):
        pass

    def load(self):
        pass

    def etl_report1(self):
        pass























