"""
Airflow DAG to orchestrate the ETL process for the Higgs Twitter dataset.
This DAG automates the process demonstrated in the ETL-download.ipynb notebook.
The DAG also sends lineage information to Apache Atlas for data governance.
"""

# from datetime import datetime, timedelta
import os
import urllib.request
import tempfile
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.param import Param

# Define the DAG
dag = DAG(
    'higgs_twitter_etl',
    # default_args=default_args,
    description='ETL process for Higgs Twitter dataset',
    schedule=None,
    # start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=['etl', 'higgs', 'twitter', 'pyspark'],
    params={
        'force_download': Param(False, type="boolean", description="Force download even if files exist"),
    }
)

# Dataset information
dataset_files = [
    "higgs-activity_time.txt.gz",
    "higgs-mention_network.edgelist.gz",
    "higgs-reply_network.edgelist.gz", 
    "higgs-retweet_network.edgelist.gz",
    "higgs-social_network.edgelist.gz"
]

def create_hdfs_dirs(**kwargs):
    """Create HDFS directories using the HDFS REST API instead of PyArrow"""
    import logging
    
    logging.info("Creating HDFS directories using REST API")
    
    try:
        # HDFS NameNode REST API endpoint
        hdfs_url = 'http://hadoop-namenode:9870/webhdfs/v1'
        
        # Directories to create
        dirs = [
            "/HiggsTwitter",
            "/HiggsTwitter/parquet",
            "/HiggsTwitter/results"
        ]
        
        # Create each directory
        for dir_path in dirs:
            # Check if directory exists
            check_url = f"{hdfs_url}{dir_path}?op=GETFILESTATUS"
            
            try:
                check_response = requests.get(check_url)
                if check_response.status_code == 200:
                    logging.info(f"Directory already exists: {dir_path}")
                    continue
            except Exception as e:
                logging.info(f"Directory does not exist or error checking: {dir_path}, {str(e)}")
            
            # Create directory using MKDIRS operation
            create_url = f"{hdfs_url}{dir_path}?op=MKDIRS&permission=777"
            response = requests.put(create_url)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('boolean', False):
                    logging.info(f"Successfully created directory: {dir_path}")
                else:
                    logging.error(f"Failed to create directory: {dir_path}")
            else:
                logging.error(f"Failed to create directory {dir_path}, status code: {response.status_code}")
                logging.error(f"Response: {response.text}")
        
        logging.info("HDFS directories created successfully")
        return True
    
    except Exception as e:
        logging.error(f"Error creating HDFS directories: {e}")
        raise



# Create the directory structure in HDFS using Python with REST API
create_hdfs_dirs = PythonOperator(
    task_id='create_hdfs_dirs',
    python_callable=create_hdfs_dirs,
    dag=dag,
)

# Define functions for the PythonOperators
def check_hdfs_files(**kwargs):
    """Check if all required dataset files exist in HDFS using REST API"""
    import logging
    import requests
    
    ti = kwargs['ti']
    force_download = kwargs['params']['force_download']
    
    if force_download:
        logging.info("Force download parameter is set to True, downloading files...")
        return 'download_files'
    
    try:
        # HDFS NameNode REST API endpoint
        hdfs_url = 'http://hadoop-namenode:9870/webhdfs/v1'
        base_dir = "/HiggsTwitter"
        missing_files = []
        
        # Check if files exist in HDFS
        for file in dataset_files:
            hdfs_path = f"{base_dir}/{file}"
            check_url = f"{hdfs_url}{hdfs_path}?op=GETFILESTATUS"
            
            try:
                response = requests.get(check_url)
                if response.status_code == 200:
                    logging.info(f"Found file {hdfs_path}")
                else:
                    logging.info(f"File {hdfs_path} not found")
                    missing_files.append(file)
            except Exception as e:
                logging.error(f"Error checking file {hdfs_path}: {e}")
                missing_files.append(file)
        
        # Store the missing files list for the next task
        ti.xcom_push(key='missing_files', value=missing_files)
        
        if missing_files:
            return 'download_files'
        else:
            return 'convert_to_parquet'
    
    except Exception as e:
        logging.error(f"Error checking HDFS files: {e}")
        # If there's an error, assume files need to be downloaded
        return 'download_files'


def download_files(**kwargs):
    """Download missing Higgs Twitter dataset files to local storage and upload to HDFS"""
    import logging
    import requests
    
    ti = kwargs['ti']
    missing_files = ti.xcom_pull(key='missing_files', task_ids='check_hdfs_files') or dataset_files
    base_url = "https://snap.stanford.edu/data/"
    temp_dir = tempfile.gettempdir()
    hdfs_rest_url = 'http://hadoop-namenode:9870/webhdfs/v1'
    
    try:
        for file in missing_files:
            # Download file
            file_url = f"{base_url}{file}"
            local_path = os.path.join(temp_dir, file)
            
            logging.info(f"Downloading {file_url} to {local_path}")
            urllib.request.urlretrieve(file_url, local_path)
            
            # Upload to HDFS via REST API
            hdfs_path = f"/HiggsTwitter/{file}"
            
            # Create file with CREATE operation - this returns a redirect URL
            create_url = f"{hdfs_rest_url}{hdfs_path}?op=CREATE&overwrite=true"
            response = requests.put(create_url, allow_redirects=False)
            
            if response.status_code == 307:
                # Get the redirect URL
                redirect_url = response.headers['Location']
                
                # Upload the file content to the redirect URL
                with open(local_path, 'rb') as file_data:
                    upload_response = requests.put(redirect_url, data=file_data)
                
                if upload_response.status_code == 201:
                    logging.info(f"Successfully uploaded {file} to HDFS")
                else:
                    logging.error(f"Failed to upload file {file}, status code: {upload_response.status_code}")
                    logging.error(f"Response: {upload_response.text}")
            else:
                logging.error(f"Failed to initiate upload for {file}, status code: {response.status_code}")
                logging.error(f"Response: {response.text}")
            
            # Remove the local temp file
            os.remove(local_path)
            logging.info(f"Removed temp file {local_path}")
        
        return True
    
    except Exception as e:
        logging.error(f"Error downloading or uploading files: {e}")
        raise


# Check if files exist in HDFS
check_hdfs_files = BranchPythonOperator(
    task_id='check_hdfs_files',
    python_callable=check_hdfs_files,
    dag=dag,
)

# Download missing files (if needed)
download_files = PythonOperator(
    task_id='download_files',
    python_callable=download_files,
    dag=dag,
)


# Create a SparkSubmitOperator for the Parquet conversion
convert_to_parquet = SparkSubmitOperator(
    task_id='convert_to_parquet',
    application="./include/scripts/convert_to_parquet.py",
    conn_id="my_spark_conn",  # Need to create this connection in Airflow
    java_class=None,  # No Java class as we're running Python
    name="higgs_parquet_conversion",
    trigger_rule="none_failed",  # Run this task when all direct upstream tasks have succeeded or been skipped
    verbose=True,
    dag=dag,
)

# Define entity for analysis results
analysis_results_path = f"hdfs://hadoop-namenode:8020/HiggsTwitter/results"


# Run Spark analysis on the Parquet files
run_analysis = SparkSubmitOperator(
    task_id='run_analysis',
    application="./include/scripts/run_analysis.py", 
    conn_id="my_spark_conn",  # Will be created by setup script
    java_class=None,  # No Java class as we're running Python
    name="higgs_analysis",
    on_execute_callback=None,  # No special pre-execution actions
    verbose=True,
    dag=dag,
)


# Define task dependencies
create_hdfs_dirs >> check_hdfs_files >> [download_files, convert_to_parquet]
download_files >> convert_to_parquet
convert_to_parquet >> run_analysis