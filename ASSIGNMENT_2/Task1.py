#!/usr/bin/env python
# coding: utf-8

# In[200]:


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import random
import os
import random
import shutil
import zipfile
import requests
from bs4 import BeautifulSoup
import requests


# In[205]:


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'fetch_save_zip_archive_ncei_data',
    default_args=default_args,
    description='Fetch and process NOAA NCEI data',
    schedule_interval=None,
)

# Base URL and year
base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"
year = input("Enter year:" )
destination_path= input("Enter moving archive destination path:")
fetch_data_page= input("Enter fetch data page path:" )

# Python function to select random data files
def select_random_data_files():
    selected_files = []
    num_files= input("Enter number of files:" )
    try:
        num_files=int(num_files)

    except Exception as e:
        print(e)
        

    with open(html_file_path, 'r') as file:
        soup = BeautifulSoup('data_page.html', 'html.parser')

        # Assuming files are represented as anchor tags (<a>) with href attribute pointing to the file
        file_tags = soup.find_all('a', href=True)

        for file_tag in file_tags:
            href = file_tag['href']

            # Add additional conditions as per your requirement
            # For example, selecting only files with specific extensions
            if re.search(r'\.csv$', href):
                selected_files.append(href)

            # Stop when 5 files are selected
            if len(selected_files) >= num_files:
                break
    print(selected_files)
    return selected_files
    
# Function to fetch the page containing locatio datasets and save individual files
def fetch_and_save_csv(base_url, year, selected_files):
    for file_name in selected_files:
        url = f"{base_url}/{year}/{file_name}"
        response = requests.get(url)
        
        # Example: save the CSV file to a directory
        with open(f"{file_name}", 'wb') as f:
            f.write(response.content)

# Function to zip the selected files

def zip_files(selected_files):
    with zipfile.ZipFile('zipped_files.zip', 'w') as zipf:
        for file_name in selected_files:
            zipf.write(file_name)


# Function to move the zip of selected files

def move_archive(destination_path):
    shutil.move('zipped_files.zip', destination_path)

# Function to fetch the page containing location-wise datasets
fetch_data_page_cmd = f"curl {base_url}{year}/ > fetch_data_page"

# Task to fetch the data page
fetch_data_page_operator = BashOperator(
    task_id='fetch_data_page',
    bash_command=fetch_data_page_cmd,
    dag=dag,
)

# Task to select random files from the data page

select_random_files_operator = PythonOperator(
    task_id='select_random_files',
    python_callable=select_random_data_files,
    dag=dag,
)

# Task to fetch the and save the individual csv files from data page

fetch_and_save_task = PythonOperator(
    task_id='fetch_and_save_csv',
    python_callable=fetch_and_save_csv,
    op_kwargs={'base_url': base_url, 'year': year, 'selected_files': selected_files},
    dag=dag
)

# Task to zip the saved csv files


zip_files_task = PythonOperator(
    task_id='zip_files',
    python_callable=zip_files,
    op_kwargs={'selected_files': selected_files},
    dag=dag
)

# Task to move the zip file

move_archive_task = PythonOperator(
    task_id='move_archive',
    python_callable=move_archive,
    op_kwargs={'destination_path': destination_path },
    dag=dag
)

# Define task dependencies
fetch_data_page_operator >> select_random_files_operator >> fetch_and_save_task >> zip_files_task >> move_archive_task
print(selected_files)


# In[206]:


dag.test()


# In[ ]:


Users(/saigowtham.t/Downloads/airflow_workspace/data_page.html)

