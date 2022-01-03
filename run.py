"""Running the Xetra ETL application"""

def main():
    # Parameters/Configurations
    # Later read config
    arg_date ='2021-12-31'
    today_str='2021-12-31'
    src_format = '%Y-%m-%d'
    src_bucket_name = 'deutsche-boerse-xetra-pds'
    trg_bucket_name = 'xetra-edp'
    columns = ['ISIN', 'Date', 'Time', 'StartPrice', 'MaxPrice', 'MinPrice', 'EndPrice', 'TradedVolume']
    trg_key = 'xetra_daily_report_'
    trg_format = '.parquet'
    meta_key='meta_file.csv'
    
    # Init
    s3 = boto3.resource('s3')
    bucket_src = s3.Bucket(src_bucket_name)
    bucket_trg = s3.Bucket(trg_bucket_name)
    
    # run application
    date_list = return_date_list(bucket_src, arg_date, src_format)
    etl_report1(bucket_src, bucket_trg, date_list, columns, arg_date, trg_key, trg_format)

if __name__ == '__main__':
    main()