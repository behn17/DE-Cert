#!/bin/bash

# Task 1.3 - unzip the data
echo "Unzipping data..."
tar -xzf /home/project/airflow/dags/tolldata.tgz


# Task 1.4 - extract data from csv file
echo "Extracting data from .csv file..."
cut -d',' -f1-4 /home/project/airflow/dags/vehicle-data.csv > /home/project/airflow/dags/csv_data.csv


# Task 1.5 - extract data from tsv file
echo "Extracting data from .tsv file..."
cut -f5-7 /home/project/airflow/dags/tollplaza-data.tsv | tr '\t' ',' | tr -d '\r' > /home/project/airflow/dags/tsv_data.csv


# Task 1.6 - extract data from fixed width file
echo "Extracting data from .txt file..."
cut -c59-67 /home/project/airflow/dags/payment-data.txt | tr ' ' ',' > /home/project/airflow/dags/fixed_width_data.csv


# Task 1.7 - consolidate data into one csv file
echo "Consolidating data into one csv file..."
paste -d',' /home/project/airflow/dags/csv_data.csv /home/project/airflow/dags/tsv_data.csv /home/project/airflow/dags/fixed_width_data.csv > /home/project/airflow/dags/extracted_data.csv


# Task 1.8 - transform and load the data
echo "Transforming data and loading to staging directory..."
awk -F',' '{print $1","$2","$3","toupper($4)","$5","$6","$7","$8","$9}' /home/project/airflow/dags/extracted_data.csv > /home/project/airflow/dags/transformed_data.csv
