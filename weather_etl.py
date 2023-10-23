# %%
#Import librareies

import requests
from datetime import date, timedelta
import pandas as pd
import s3fs 
import boto3
from io import StringIO

# %%
def get_air_temperature(api_url,formatted_date):

    df= pd.DataFrame()

     # Make a GET request to the API
    response = requests.get(api_url)
    print(response)

    # Check if the request was successful (HTTP status code 200)
    if response.status_code == 200:
        # The response contains JSON data, so you can parse it using the json() method
        data = response.json()
        meta_data =data['metadata']
        stations = meta_data['stations']

        if len(stations) > 0:
            reading_type = meta_data['reading_type']
            reading_unit = meta_data['reading_unit']

            # Create a list to store the extracted data
            temperature_data = []

            # Iterate over the readings and extract station_id and value
            for reading in data['items'][0]['readings']:
                station_id = reading['station_id']
                temperature_value = reading['value']

                # Find the station details
                station_details = next((station for station in stations if station['id'] == station_id), None)

                # Append data to the list
                if station_details:
                    temperature_data.append({
                        'Date': formatted_date,
                        'Station_ID': station_id,
                        'Device_ID': station_details['device_id'],
                        'Name': station_details['name'],
                        'Latitude': station_details['location']['latitude'],
                        'Longitude': station_details['location']['longitude'],
                        'Reading_Type': reading_type,
                        'Reading_Unit': reading_unit,
                        'Temperature_Celsius': temperature_value
                    })
            
            # Create a DataFrame from the extracted data
            df = pd.DataFrame(temperature_data)
    
        
    else:
        # If the request was not successful, you can handle the error here
        print(f"Error. Status code: {response.status_code}")


    return df 

# %%
def get_rainfall(api_url,formatted_date):

    df= pd.DataFrame()

     # Make a GET request to the API
    response = requests.get(api_url)

    # Check if the request was successful (HTTP status code 200)
    if response.status_code == 200:
        # The response contains JSON data, so you can parse it using the json() method
        data = response.json()
        meta_data =data['metadata']
        stations = meta_data['stations']

        if len(stations) > 0:
            reading_type = meta_data['reading_type']
            reading_unit = meta_data['reading_unit']

            # Create a list to store the extracted data
            rainfall_data = []

            # Iterate over the readings and extract station_id and value
            for reading in data['items'][0]['readings']:
                station_id = reading['station_id']
                rainfall_value = reading['value']

                # Find the station details
                station_details = next((station for station in stations if station['id'] == station_id), None)

                # Append data to the list
                if station_details:
                    rainfall_data.append({
                        'Date': formatted_date,
                        'Station_ID': station_id,
                        'Device_ID': station_details['device_id'],
                        'Name': station_details['name'],
                        'Latitude': station_details['location']['latitude'],
                        'Longitude': station_details['location']['longitude'],
                        'Reading_Type': reading_type,
                        'Reading_Unit': reading_unit,
                        'Rainfall_mm': rainfall_value
                    })
            
            # Create a DataFrame from the extracted data
            df = pd.DataFrame(rainfall_data)
    
        
    else:
        # If the request was not successful, you can handle the error here
        print(f"Error. Status code: {response.status_code}")


    return df 

# %%
def get_humidity(api_url,formatted_date):

    df= pd.DataFrame()

     # Make a GET request to the API
    response = requests.get(api_url)

    # Check if the request was successful (HTTP status code 200)
    if response.status_code == 200:
        # The response contains JSON data, so you can parse it using the json() method
        data = response.json()
        meta_data =data['metadata']
        stations = meta_data['stations']

        if len(stations) > 0:
            reading_type = meta_data['reading_type']
            reading_unit = meta_data['reading_unit']

            # Create a list to store the extracted data
            humidity_data = []

            # Iterate over the readings and extract station_id and value
            for reading in data['items'][0]['readings']:
                station_id = reading['station_id']
                humidity_value = reading['value']

                # Find the station details
                station_details = next((station for station in stations if station['id'] == station_id), None)

                # Append data to the list
                if station_details:
                    humidity_data.append({
                        'Date': formatted_date,
                        'Station_ID': station_id,
                        'Device_ID': station_details['device_id'],
                        'Name': station_details['name'],
                        'Latitude': station_details['location']['latitude'],
                        'Longitude': station_details['location']['longitude'],
                        'Reading_Type': reading_type,
                        'Reading_Unit': reading_unit,
                        'Humidity_Percentage': humidity_value
                    })
            
            # Create a DataFrame from the extracted data
            df = pd.DataFrame(humidity_data)
    
        
    else:
        # If the request was not successful, you can handle the error here
        print(f"Error. Status code: {response.status_code}")


    return df 

# %%
def  run_weather_etl():

    bucket_name = 'airflow-singapore-weather-bucket'
    file_key = 'singapore_weather_data.csv'


    # Create an S3 client
    s3 = boto3.client('s3')

    # Download the existing CSV file from S3 into a Pandas DataFrame
    s3_response = s3.get_object(Bucket=bucket_name, Key=file_key)
    csv_content = s3_response['Body'].read().decode('utf-8')


    existing_weather_df = pd.read_csv(StringIO(csv_content),index_col=False)
    existing_weather_df.reset_index(drop=True, inplace=True)
    print(existing_weather_df)
    max_date = existing_weather_df['Date'].max()
    # Convert max_date from a string to a date object
    max_date = pd.to_datetime(max_date).date()

    # Define the API URL
    temperature_base_url = "https://api.data.gov.sg/v1/environment/air-temperature"
    rainfall_base_url = "https://api.data.gov.sg/v1/environment/rainfall"
    humidity_base_url = "https://api.data.gov.sg/v1/environment/relative-humidity"


    air_temp_df = pd.DataFrame()
    rainfall_df = pd.DataFrame()
    humidity_df = pd.DataFrame()

    # Add one day to max_date
    start_date = max_date + timedelta(days=1)

 
    # Calculate today's date

    end_date = date(2023, 10, 12 )

    # Loop through the date range and make requests for each date
    current_date = start_date

 
    while current_date <= end_date:

        print(current_date)
        # Format the date in YYYY-MM-DD format
        formatted_date = current_date.strftime("%Y-%m-%d")

        # Create the full URL with the formatted date as a parameter
        temperature_url = f"{temperature_base_url}?date={formatted_date}"
        rainfall_url = f"{rainfall_base_url}?date={formatted_date}"
        humidity_url = f"{humidity_base_url}?date={formatted_date}"

        
        air_temp_df= pd.concat( [ air_temp_df,get_air_temperature(temperature_url,formatted_date)],ignore_index=True)
        rainfall_df= pd.concat( [ rainfall_df,get_rainfall(rainfall_url,formatted_date)],ignore_index=True)
        humidity_df= pd.concat( [ humidity_df,get_humidity(humidity_url,formatted_date)],ignore_index=True)


        air_temp_df.reset_index(drop=True, inplace=True)
        rainfall_df.reset_index(drop=True, inplace=True)
        humidity_df.reset_index(drop=True, inplace=True)
   
      
        # Move to the next date
        current_date += timedelta(days=1)

    new_weather_df = pd.concat([air_temp_df, rainfall_df,humidity_df], ignore_index=True)
    new_weather_df.reset_index(drop=True, inplace=True)

    # Concatenate the existing and new DataFrames
    combined_data_df = pd.concat([existing_weather_df, new_weather_df], ignore_index= True)
    combined_data_df.to_csv("s3://airflow-singapore-weather-bucket/singapore_weather_data.csv",index=False)

    
    print("Insert operation completed.")


    




# %%



