"""
File to store constants
"""
from enum import Enum

# TWO CLASSES ARE CHILD CLASS OF Enum CLASS, WHICH DEFINES CONSTANTS - CLASS ATTRIBUTES
class s3FileTypes(Enum):
    """
    supported file types for s3BucketConnector
    """
    CAV = 'csv'
    PARQUET = 'parquet'

    class MetaProcessFormat(Enum):
        """
        formation for MetaProcess class
        """
        META_DATA_FORMAT = '%Y-%m-%d'
        META_PROCESS_DATE_FORMAT = '%Y-%M-%d %H:%M:%S'
        META_SOURCE_DATE_COL = 'source_date'
        META_PROCESS_COL = 'datetime_of_processing'
        META_FILE_FORMAT = 'csv'