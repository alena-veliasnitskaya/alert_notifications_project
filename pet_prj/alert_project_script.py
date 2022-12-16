from os.path import exists
from time import sleep
import dask.dataframe as dd
from datetime import datetime


def check_if_file_exists(path, filter_number):
    file_exists = exists(path)
    try:
        if file_exists:
            transformation_file(path, filter_number)
        else:
            print('waiting for a log')
            sleep(10)
            check_if_file_exists(path, filter_number)
    except IOError as e:
        print(e)


def transformation_file(path, filter_number):
    transformation_type = filter_number
    df = dd.read_csv(path)
    df.columns = ['error_code', 'error_message', 'severity', 'log_location', 'mode', 'model', 'graphics', 'session_id',
                  'sdkv', 'test_mode', 'flow_id', 'flow_type', 'sdk_date', 'publisher_id', 'game_id', 'bundle_id',
                  'appv', 'language', 'os', 'adv_id', 'gdpr', 'ccpa', 'country_code', 'date']
    if transformation_type == '1':
        def from_unix(x):
            return datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M')
    elif transformation_type == '2':
        def from_unix(x):
            return datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:00')
    df.date = df["date"].map(from_unix)
    filter_errors = df[df['severity'] != 'Success']
    df_filter = filter_errors[['date', 'bundle_id']].copy()
    if transformation_type == '1':
        log_file = df_filter.groupby('date').count()
        log_file = log_file[log_file['bundle_id'] > 10]
    elif transformation_type == '2':
        log_file = df_filter.groupby(['date', 'bundle_id'])['bundle_id'].count()
        log_file = log_file.to_frame().rename(
            columns={'date': 'hours', 'bundle_id': 'error_count', 'count': 'error_count'}).reset_index()
        log_file[log_file['error_count'] > 10]

    output_path = '/usr/src/app/data/out/alert_log_file-*.csv'
    send_notification(log_file, filter_number, output_path)


def send_notification(df, filter_number, output_path):
    try:
        if filter_number == '1':
            df.to_csv(output_path)
            print('your alert log has been uploaded, please wait for the second one to be added')
        if filter_number == '2':
            df.to_csv(output_path, mode='a')
            print('your alert log has been updated')
    except IOError as e:
        print(e)


if __name__ == '__main__':
    input_path = '/usr/src/app/data/in/data.csv'
    check_if_file_exists(input_path, '1')
    check_if_file_exists(input_path, '2')
