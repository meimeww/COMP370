import argparse
import pandas as pd
import dask.dataframe as dd


def borough_complaints(input_file, start_date, end_date, output_file=None):
    usecols = [0, 1, 2, 5, 25]  # Only load necessary columns
    column_names = ['unique_key', 'created_date', 'closed_date', 'complaint_type', 'borough']
    dtypes = {
        'unique_key': 'object',
        'created_date': 'object',
        'closed_date': 'object',
        'complaint_type': 'object',
        'borough': 'object'
    }

    df = dd.read_csv(input_file, header=None, names=column_names, dtype=dtypes, usecols=usecols)

    date_format = '%m/%d/%Y %I:%M:%S %p'

    df['created_date'] = dd.to_datetime(df['created_date'], format=date_format, errors='coerce')
    df['closed_date'] = dd.to_datetime(df['closed_date'], format=date_format, errors='coerce')

    start_date = pd.to_datetime(start_date, format="%Y-%m-%d")
    end_date = pd.to_datetime(end_date, format="%Y-%m-%d")

    df_filtered = df[(df['created_date'] >= start_date) & (df['created_date'] <= end_date)]

    result = df_filtered.groupby(['complaint_type', 'borough']).size().reset_index()

    result = result.rename(columns={0: 'count'}).compute()

    result_csv = result.to_csv(index=False)

    if output_file:
        with open(output_file, 'w') as f:
            f.write(result_csv)
    else:
        print(result_csv)
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', required=True, help='input_csv')
    parser.add_argument('-s', '--start', required=True, help='start_date')
    parser.add_argument('-e', '--end', required=True, help='end_date')
    parser.add_argument('-o', '--output', help='output_csv')
    
    args = parser.parse_args()
    borough_complaints(args.input, args.start, args.end, args.output)