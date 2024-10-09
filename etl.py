"""
_summary_: Extract and transform data from two different sources

Returns:
    _type_: tuple
    _description_: A tuple containing the transformed data
"""

import pandas as pd
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# NEWYORK_DATA = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"
# JOHN_DATA = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv"

def extract_transform(ny_url, jh_url, output_file):
    """
    _summary_: Extract and transform data from two different sources

    Args:
        newyork_data (_type_): Provide the URL of the New York COVID-19 data
        john_data (_type_): Provide the URL of the John Hopkins COVID-19 data

    Returns:
        _type_: tuple
    """
    try:
        # Load the data
        ny_data = pd.read_csv(ny_url)
        jh_data = pd.read_csv(jh_url)
        logger.info(f"NY Data: {ny_data.head()}")
        logger.info(f"JH Data: {jh_data.head()}")

        # Transform the newyork data
        ny_data = ny_data.astype(
            {
                'date': 'datetime64[ns]',
                'cases': 'int64',
                'deaths': 'int64'
            }
        )

        # Transform the john data
        jh_data = jh_data[jh_data['Country/Region'] == 'US']
        jh_data = jh_data[['Date', 'Recovered']]

        jh_data = jh_data.astype(
            {
                'Date': 'datetime64[ns]',
                'Recovered': 'int64',
            }
        )
        jh_data = jh_data.rename(
            columns = {
                'Date': 'date',
                'Recovered': 'recovered',
            }
        )

        # Merge the data
        merged_data = pd.merge(
            ny_data,
            jh_data,
            on='date'
        )
        logger.info(f"Merged Data: {merged_data.head()}")

        merged_data.to_csv(output_file, index=False)

        return merged_data

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return None

# final_data = extract_transform(NEWYORK_DATA, JOHN_DATA, 'merged_covid_data.csv')
# print(final_data)
