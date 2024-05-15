import json
import boto3
import os
import pandas as pd
import numpy as np
import requests
import datetime
from io import StringIO

s3 = boto3.client('s3')

def lambda_handler(event, context):
	Bucket ='kevmer13bucket1'
	CSVFileName = 'WeatherData.csv'
	Temps_Extractor(Bucket, CSVFileName)	
	print('File updated')



def Temps_Extractor(BucketName, CSVFileName):
	API_Key = '5a82762e14cec1f96b05a8f8fa9120f4'
	cities = ['Mumbai', 'London', 'Manchester', 'Delhi', 'Colombo', 'New York', 'Chicago', 'Hong Kong', 'Bangkok', 'Cairo', 'Los Angeles', 'Paris', 'Berlin', 'Oslo', 'Helsinki', 'Lagos', 'Cape Town', 'Karachi', 'Moscow', 'Tokyo', 'Sydney', 'Melbourne, AU', 'Auckland', 'Barcelona', 'Madrid', 'Toronto', 'Vancouver', 'Mexico City', 'Sao Paulo', 'Rio De Janeiro', 'Buenos Aires', 'Nairobi', 'Dar es Salaam', 'Marrakesh', 'Dubai', 'Riyadh', 'Doha', 'Istanbul', 'Beijing', 'Shanghai', 'Seoul', 'Kolkata', 'Bangalore', 'Singapore', 'Kuala Lumpur', 'Perth', 'Manila', 'Jakarta', 'Montreal', 'Rome, IT']

	date_time = []
	temps = []	

	for i in range(len(cities)):   #We get the new data in this loop
	    response = requests.get(f'http://api.openweathermap.org/data/2.5/weather?q={cities[i]}&appid={API_Key}')
	    if response.status_code == 200:
	        APIData = response.json()
	        temps.append(APIData['main']['temp'])
	        dateData = datetime.datetime.fromtimestamp(APIData['dt'])
	        date_time.append(dateData)
	    else:
	        print('Error')

	dict = {'City': cities,
	        'Temp': temps,
	        'Date_Time': date_time
	       }

	df = pd.DataFrame(dict)

	df['Dates'] = pd.to_datetime(df['Date_Time']).dt.date
	df['Time'] = pd.to_datetime(df['Date_Time']).dt.time
	df_new = df.drop('Date_Time', axis=1)  #Dataframe with new temps is ready here

	obj = s3.get_object(Bucket=BucketName, Key=CSVFileName)  #Reading csv file from the S3
	df_old = pd.read_csv(obj['Body'])

	updated_df = pd.concat([df_old, df_new], ignore_index = True)  #Joining the dataframes
	csv_buffer = StringIO()
	updated_df.to_csv(csv_buffer, index=False)	
	s3_resource = boto3.resource('s3')
	s3_resource.Object(BucketName, 'WeatherData.csv').put(Body=csv_buffer.getvalue())  #Actually uploading to S3
