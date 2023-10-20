import requests
import pandas as pd
import urllib.parse
import time
import config
import os
import numpy as np
# Importing Solar-TK functions and functions I used for CSV Processing
from zipfile import ZipFile
from urllib.request import urlretrieve
from sunpos import sunpos
#from solartk.maximum_generation import compute_max_power
#from solartk.sunpos import get_sun_position


TEMP_DIR = os.path.join(os.path.expanduser("~"), "temp")
SOLAR_DATA_DIR = os.path.join(TEMP_DIR, "solar_data")

if not os.path.exists(TEMP_DIR):
    os.mkdir(TEMP_DIR)
if not os.path.exists(SOLAR_DATA_DIR):
    os.mkdir(SOLAR_DATA_DIR)


BASE_URL = "https://developer.nrel.gov/api/nsrdb/v2/solar/psm3-5min-download.json?"
POINTS = ['2277372']


class SolarTKMaxPowerCalculator:
    def __init__(self, tilt, orientation, k, c=0.05, t_baseline=25):
        self.tilt_ = tilt
        self.orientation = orientation
        self.k = k
        self.c = c
        self.t_baseline = t_baseline
    
    @staticmethod
    def get_sun_position(times, latitude, longitude, sun_position_method='psa'):
        if sun_position_method == 'psa':
            df = times.to_frame(index=False)
            df.columns = ['time']
            df[['azimuth', 'zenith']] = df['time'].apply(lambda x: sunpos(x, latitude, longitude))
            return df
        else:
            raise ValueError('Invalid argument for sun_position_method variable.')
    
    def compute_sun_position(self, times, latitude, longitude):
        """Computes sun's azimuth and zenith angles for given times and coordinates."""
        sun_positions = self.get_sun_position(times, latitude, longitude)
        return sun_positions
    
    def compute_max_power(self, df, sun_position):
        clearsky_irradiance = df.copy()
        # make the index a column called 'datetime'
        clearsky_irradiance.reset_index(inplace=True)
        clearsky_irradiance['max_power'] = clearsky_irradiance['DNI'] * self.k * (
            1 + self.c * (self.t_baseline - 0)) * (
            np.cos(np.radians(90) - pd.to_numeric(sun_position['zenith'])) *
            np.sin(self.tilt_) *
            np.cos(pd.to_numeric(sun_position['azimuth']) - self.orientation) +
            np.sin(np.radians(90) - pd.to_numeric(sun_position['zenith'])) *
            np.cos(self.tilt_))
        max_generation = clearsky_irradiance[['datetime', 'max_power']]
        max_generation.columns = ['#time', 'max_generation']
        return max_generation
    
    
#code from the CSV prcoessing
def load_and_concatenate_csvs(folder):
    df_list = []
    for nested_folder in os.listdir(folder):
        nested_path = os.path.join(folder, nested_folder)
        for file in os.listdir(nested_path):
            if file.endswith('.csv'):
                csv_path = os.path.join(nested_path, file)
                skip = 2
                # open the csv at csv_path using pandas
                df = pd.read_csv(csv_path, skiprows=skip)
                # append the dataframe to df_list
                df_list.append(df)
    df = pd.concat(df_list, ignore_index=True)
    return df


def download_file(url, destination):
    """Download a file from a URL to a given destination."""
    response = requests.get(url)
    with open(destination, 'wb') as file:
        file.write(response.content)


def unzip_file(file_path, destination):
    """Unzip a file to a given destination."""
    with ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(destination)


def cleanup():
    """Delete temporary files after processing."""
    for filename in os.listdir(SOLAR_DATA_DIR):
        file_path = os.path.join(SOLAR_DATA_DIR, filename)
        os.remove(file_path)


#take year as an an additional input
def fetch_download_url(attributes, interval, latitude, longitude, year):
    input_data = {
        'attributes': ','.join(attributes),
        'interval': interval,
        'to_utc': 'false',
        'wkt': 'POINT({:.4f} {:.4f})'.format(longitude, latitude),
        'api_key': config.API_KEY,
        'email': config.EMAIL,
        'names': year
    }
    headers = {'x-api-key': config.API_KEY}
    response = requests.post(BASE_URL, params=input_data,headers=headers)
    response_data = get_response_json_and_handle_errors(response)
    return response_data['outputs']['downloadUrl']


def main():
    # Get user input for the years of data
    years = input("Enter the year(s) for which you want data (e.g., 2021): ")
    # You can add validation here to check if the input is valid
    # Get user input for data types
    data_types = input("Enter data types separated by commas (e.g., ghi,dni,dew_point): ").split(',')
    # You can split the input into a list of data types
    # Get user input for the interval
    interval = input("Enter the data interval (5, 15, 30, or 60 minutes): ")
    # You can add validation here to check if the input is valid
    # Get user input for latitude and longitude
    latitude = float(input("Enter the latitude: "))
    longitude = float(input("Enter the longitude: "))

    for year in years.split(','):
        year.replace(" ", "")
        download_url = fetch_download_url(data_types, interval, latitude, longitude, years)
        zip_file_path = os.path.join(TEMP_DIR, "solar_data_{}.zip".format(year))
        download_file(download_url, zip_file_path)
        unzip_file(zip_file_path, SOLAR_DATA_DIR)
    
    df = load_and_concatenate_csvs(SOLAR_DATA_DIR)
    df['datetime'] = pd.to_datetime(df[['Year', 'Month', 'Day', 'Hour', 'Minute']])
    df.drop(['Year', 'Month', 'Day', 'Hour', 'Minute'], axis=1, inplace=True)
    df.set_index('datetime', inplace=True)
    # Compute solar generation using the provided SolarTKMaxPowerCalculator class
    gen_potential = SolarTKMaxPowerCalculator(tilt=34.5, orientation=180, k=1.0)

    sun_position = gen_potential.compute_sun_position(df.index, latitude, longitude)
    max_gen_df = gen_potential.compute_max_power(df, sun_position)

    # Merge the max_generation into the main dataframe
    df = df.merge(max_gen_df, left_index=True, right_on="#time", how="left")
    # rename max_generation
    df.rename(columns={'#time': 'datetime'}, inplace=True)
    df.set_index('datetime', inplace=True)
    df.rename(columns={'max_generation': 'Solar Generation (kWh)'}, inplace=True)
    # cleanup()
    # dump df to csv
    df.to_csv('{}_{}_solar_generation.csv'.format(latitude, longitude), index=True)

def get_response_json_and_handle_errors(response):
    if response.status_code != 200:
        print("An error has occurred with the server or the request. The request response code/status: {} {}".format(response.status_code, response.reason))
        print("The response body: {}".format(response.text))
        exit(1)
    try:
        response_json = response.json()
    except:
        print("The response couldn't be parsed as JSON, likely an issue with the server, here is the text: {}".format(response.text))
        exit(1)
    if len(response_json['errors']) > 0:
        errors = '\n'.join(response_json['errors'])
        print("The request errored out, here are the errors: {}".format(errors))
        exit(1)
    return response_json

if __name__ == "__main__":
    main()