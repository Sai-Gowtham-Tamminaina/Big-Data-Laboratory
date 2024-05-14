#!/usr/bin/env python
# coding: utf-8

# In[1]:


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import random
import os
import shutil
import zipfile
import requests
import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
import logging
import time
from airflow import DAG
from airflow.sensors.filesystem import FileSensor


# In[32]:


csv_archive_path= input("Enter zipped csv path:" )
unzip_folder_path=input("Enter unzip folder path:" )
choose_file_for_processing = input("Enter path of file in the unzipped csv folder:")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 2),  # Specify the start date here
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

# Functions to preprocess the selected file from the unzipped file


def process_csv_element(element):
    # Extract the required fields such as Windspeed, BulbTemperature, Lat, Lon
    windspeed = element['HourlyWindSpeed']
    bulb_temperature = element['HourlyDryBulbTemperature']
    lat = element['LATITUDE']
    lon = element['LONGITUDE']

    # Create a tuple of the form <Lat, Lon, [[ArrayOfHourlyDataOfTheReqFields]]>
    hourly_data = [[windspeed, bulb_temperature]]  # Example hourly data, adjust based on actual fields
    result_tuple = (lat, lon, hourly_data)

    return result_tuple

def process_csv_file(input_file):
    # Read the CSV file into a Pandas DataFrame
    df = pd.read_csv(input_file)

    # Filter the DataFrame based on required fields like Windspeed or BulbTemperature
    filtered_df = df[['LATITUDE', 'LONGITUDE','HourlyDryBulbTemperature', 'HourlyWindSpeed']]  # Adjust columns as needed

    # Convert the filtered DataFrame to a list of dictionaries for Apache Beam processing
    data_list = filtered_df.to_dict(orient='records')

    return data_list

def run_pipeline(input_file):
    with beam.Pipeline() as pipeline:
        csv_data = pipeline | 'ReadFromText' >> beam.Create(process_csv_file(input_file))
        
        processed_data = csv_data | 'ProcessCSVElement' >> beam.Map(process_csv_element)
        
        # You can further process or write the processed data as needed in your pipeline

with DAG('archive_processing_dag', default_args=default_args, schedule_interval=None) as dag:
    
    # Task to wait for the archive file to be available with a timeout of 5 seconds
    wait_for_archive = FileSensor(
        task_id='wait_for_archive',
        filepath=csv_archive_path,
        poke_interval=10,  # Check every 10 seconds
        timeout=5,  # Timeout after 5 seconds
        mode='poke'  # Use poke mode for checking file availability
    )
    
    # Task to check if the file is a valid archive and unzip its contents into individual CSV files
    check_and_unzip = BashOperator(
        task_id='check_and_unzip',
        bash_command='if [ -f csv_archive_path ]; then echo "Valid archive"; unzip csv_archive_path -d unzip_folder_path; else echo "Invalid archive"; fi',
    )

    # Task to extract and process CSV file
    task_extract_process_csv = PythonOperator(
        task_id='extract_process_csv',
        python_callable=run_pipeline,
        op_args=[choose_file_for_processing]
    )

    
    wait_for_archive >> check_and_unzip >> task_extract_process_csv







# In[33]:


dag.test()

