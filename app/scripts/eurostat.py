import pandasdmx as sdmx
import pandas as pd
from datetime import datetime, timedelta


def fetch_unemployment_data():
    # Create a client instance
    client = sdmx.Request('ESTAT')

    # Get metadata for the 'ei_lmhr_m' dataset (Unemployment rate)
    metadata_response = client.dataflow('EI_LMHR_M')
    metadata = metadata_response.dataflow['EI_LMHR_M']
    print("Metadata for 'EI_LMHR_M':")
    print(metadata)

    # Fetch the data for the 'ei_lmhr_m' dataset with the key for Sweden ('SE')
    response = client.data(resource_id='EI_LMHR_M', key={'geo': 'SE'})

    # Convert the response to a pandas DataFrame
    df = response.to_pandas().replace('', pd.NA).ffill()
    print(df.head())
    print(list(df.index))
    df = df.reset_index()
    df['TIME_PERIOD'] = pd.to_datetime(df['TIME_PERIOD'], format='%Y-%m')

    # Extract the 'TIME_PERIOD' and convert it to datetime
    df['TIME_PERIOD'] = pd.to_datetime(df['TIME_PERIOD'], format='%Y-%m')

    # Get the current date and calculate the start date for the last year
    current_date = datetime.now()
    start_date = datetime(current_date.year - 1, current_date.month, 1)

    # Filter the DataFrame for the last year
    filtered_df = df[df['TIME_PERIOD'] >= start_date]

    # Further filter the DataFrame for the specified 'indic' values
    filtered_indic_df = filtered_df[filtered_df['indic'].isin(['LM-UN-F-TOT', 'LM-UN-M-TOT'])]

    # Group by 'TIME_PERIOD' and calculate the average 'value'
    average_df = filtered_indic_df.groupby('TIME_PERIOD')['value'].mean().reset_index()

    # Rename the 'value' column to 'average_unemployment_rate'
    average_df.rename(columns={'value': 'average_unemployment_rate'}, inplace=True)
    return average_df


def list_datasets():
    client = sdmx.Request('ESTAT')
    datasets_response = client.dataflow()
    datasets = datasets_response.dataflow
    for key in datasets.keys():
        print(key, datasets[key].name)


if __name__ == "__main__":
    # list_datasets()
    unemployment_data = fetch_unemployment_data()
    unemployment_data.to_csv('eurostat_unemployment_data_se.csv', index=False)
