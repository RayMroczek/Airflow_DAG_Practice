import os
import shutil

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def csv_and_excel_mover():
    # Define the paths of your source and destination folders
    src_folder = '/Users/mrocz/Desktop/file_drop/'
    dst_folder = '/Users/mrocz/Desktop/file_destination/'
        # Get a list of all files in the source folder
    files = os.listdir(src_folder)
    #if there are no files in the source folder, don't do anything
    if len(files) == 0:
        print("no files of any kind found")
    else:
        # Loop through the source folder to see if it's in the destination folder.
        for file in files:
            if (file.endswith('.csv') or file.endswith('.xls')) and not os.path.exists(os.path.join(dst_folder, file)):
                shutil.move(os.path.join(src_folder, file), dst_folder)
                print(f"{file} has been moved to {dst_folder}")
            else:
                print("No files moved")

with DAG(
    #keep dag id same as file name for ease.
    dag_id="csv_xls_mover_dag",
    #schedule when it runs - you can use a CRON expression if you want
    schedule_interval="@daily",
    #default is already false, but specifying for clarity.
    default_args={
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2023,3,31),
    },
    #default is already false, but specifying for clarity.
    catchup=False
) as dag:

    #PythonOperator lets you execute arbitrary Python code including external libraries, reading and writing files, interacting with databases, etc.
    csv_xls_mover_dag_execute =  PythonOperator(
        #again, good practice to give the task the same id as the function name.
        task_id="csv_xls_mover_Dag_execute",
        python_callable=csv_and_excel_mover
)

#dag was having isuses locating the file, while code works independently. will have to work on this.
