"""
Methods for processing the meta file
"""
import collections
from datetime import datetime, timedelta

import pandas as pd

from source.common.s3 import S3BucketConnector
from source.common.constants import MetaProcessFormat
from source.common.custom_exceptions import WrongMetaFileException

class MetaProcess():
    """
    class for working with the meta file
    """

    @staticmethod

    def update_meta_file(bucket_meta: S3BucketConnector, meta_key: str, extract_date_list: list):
        """
        Updating the meta file with the processed Xetra dates and todays date as processed date
        :param: extract_date_list -> a list of dates that are extracted from the source
        :param: meta_key -> key of the meta file on the S3 bucket
        :param: s3_bucket_meta -> S3BucketConnector for the bucket with the meta file
        """
        # Creating an empty DataFrame using the meta file column names        
        df_new = pd.DataFrame(columns=[MetaProcessFormat.META_SOURCE_DATE_COL.value, 
                                        MetaProcessFormat.META_PROCESS_COL.value])
        # Filling the date column with extract_date_list
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list
        df_new[MetaProcessFormat.META_PROCESS_COL.value]=datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        try:
            # If meta file exists, then union DataFrame of old and new meta data is created
            df_old = bucket_meta.read_csv_to_df(meta_key)
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                raise WrongMetaFileException
            df_all = pd.concat([df_old, df_new])
        except bucket_meta.session.client('s3').exception.NoSuchKey:
            df_all=df_new
        # Writing to S3
        bucket_meta.write_df_to_s3(df_all, meta_key, MetaProcessFormat.META_FILE_FORMAT.value)
        return True


    @staticmethod
    def return_date_list(first_date: str, meta_key: str, bucket_meta: S3BucketConnector):
        """
        Creating a list of dates based on the input first_date and the already
        processed dates in the meta file
        :param: first_date -> the earliest date Xetra data should be processed
        :param: meta_key -> key of the meta file on the S3 bucket
        :param: s3_bucket_meta -> S3BucketConnector for the bucket with the meta file
        returns:
          min_date: first date that should be processed
          return_date_list: list of all dates from min_date till today
        """
        start = datetime.strptime(first_date, MetaProcessFormat.META_DATE_FORMAT.value)\
                                                            .date() - timedelta(days=1)
        today = datetime.today().date()
        try:
            df_meta = bucket_meta.read_csv_to_df(meta_key)
            return_date_list = [(start+timedelta(days=x)) for x in range(0, (today-start).days +1)]
            sr_dates = set(pd.to_datetime(df_meta['source_date']).dt.date)
            process_dates = set(return_date_list[1:])
            if process_dates-sr_dates:
                mindate=min(process_dates-sr_dates)-timedelta(days=1)
                return_min_date=(mindate + timedelta(days=1))\
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                return_dates = [date.strftime(MetaProcessFormat.META_DATE_FORMAT.value) \
                    for date in return_date_list if date>=mindate]
            else:
                return_dates = []
                return_min_date=datetime(3300, 1, 1).date()
        except bucket_meta.session.client('s3').execptions.NoSuchKey:
            return_min_date = first_date
            return_dates = [(start + timedelta(day=x)).strftime(MetaProcessFormat.META_DATE_FORMAT.value) \
                            for x in range(0, (today-start).days + 1)]
        return return_min_date, return_dates