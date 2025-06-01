"""
PySpark script to convert Higgs Twitter dataset files to Parquet format.
This script automates the conversion process demonstrated in the ETL-download.ipynb notebook.
"""

import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import subprocess


# Add performance monitoring
class SparkJobMonitor:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.start_time = None
        self.metrics = {}
    
    def start_job(self, job_name):
        self.start_time = time.time()
        self.job_name = job_name
        logging.info(f"Starting job {job_name} at {self.start_time}")
    
    def end_job(self):
        end_time = time.time()
        duration = end_time - self.start_time
        self.metrics['job_duration'] = duration
        logging.info(f"Job {self.job_name} completed in {duration:.2f} seconds")
        return duration
    
    def log_stage_metrics(self):
        """Log Spark stage metrics for overhead calculation"""
        try:
            sc = self.spark.sparkContext
            status_tracker = sc.statusTracker()
            
            # Get job info
            job_ids = status_tracker.getJobIds()
            for job_id in job_ids:
                job_info = status_tracker.getJobInfo(job_id)
                if job_info:
                    logging.info(f"Job {job_id}: Status={job_info.status}, Stages={len(job_info.stageIds)}")
                    
        except Exception as e:
            logging.error(f"Error getting stage metrics: {e}")

def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Higgs Twitter - Convert to Parquet") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()
    
    # Set the HDFS paths
    hdfs_dir = "hdfs://hadoop-namenode:8020/HiggsTwitter/"
    parquet_dir = "hdfs://hadoop-namenode:8020/HiggsTwitter/parquet/"
    
    # File definitions with their schemas for conversion
    file_definitions = [
        {
            "name": "social_network",
            "source": hdfs_dir + "higgs-social_network.edgelist.gz",
            "target": parquet_dir + "social_network",
            "schema": StructType([
                StructField("follower", IntegerType()), 
                StructField("followed", IntegerType())
            ]),
            "separator": " "
        },
        {
            "name": "retweet_network",
            "source": hdfs_dir + "higgs-retweet_network.edgelist.gz",
            "target": parquet_dir + "retweet_network",
            "schema": StructType([
                StructField("tweeter", IntegerType()), 
                StructField("tweeted", IntegerType()), 
                StructField("occur", IntegerType())
            ]),
            "separator": " "
        },
        {
            "name": "reply_network",
            "source": hdfs_dir + "higgs-reply_network.edgelist.gz",
            "target": parquet_dir + "reply_network",
            "schema": StructType([
                StructField("replier", IntegerType()), 
                StructField("replied", IntegerType()), 
                StructField("occur", IntegerType())
            ]),
            "separator": " "
        },
        {
            "name": "mention_network",
            "source": hdfs_dir + "higgs-mention_network.edgelist.gz",
            "target": parquet_dir + "mention_network",
            "schema": StructType([
                StructField("mentioner", IntegerType()), 
                StructField("mentioned", IntegerType()), 
                StructField("occur", IntegerType())
            ]),
            "separator": " "
        },
        {
            "name": "activity_time",
            "source": hdfs_dir + "higgs-activity_time.txt.gz",
            "target": parquet_dir + "activity_time",
            "schema": StructType([
                StructField("userA", IntegerType()),
                StructField("userB", IntegerType()),
                StructField("timestamp", IntegerType()),
                StructField("interaction", StringType())
            ]),
            "separator": " "
        }
    ]
    
    # Convert each file to Parquet format
    for file_def in file_definitions:
        print(f"Converting {file_def['name']} to Parquet format...")
        
        # Read CSV file using the provided schema
        df = spark.read.csv(
            path=file_def["source"], 
            sep=file_def["separator"], 
            schema=file_def["schema"]
        )
        
        # Write to Parquet format with overwrite mode
        df.write.mode("overwrite").parquet(file_def["target"])
        
        print(f"Converted {file_def['name']} successfully.")

         # Set permissions to 777 using HDFS chmod command
        # Extract the target path without the hdfs:// prefix for the chmod command
        target_path = file_def["target"].replace("hdfs://hadoop-namenode:8020", "")
        chmod_cmd = f"hadoop fs -chmod -R 777 {target_path}"
        print(f"Setting permissions with: {chmod_cmd}")
        
        try:
            subprocess.run(chmod_cmd, shell=True, check=True)
            print(f"Successfully set permissions to 777 for {file_def['name']} parquet files")
        except subprocess.CalledProcessError as e:
            print(f"Error setting permissions for {file_def['name']}: {e}")

    
    # Stop the SparkSession
    spark.stop()
    print("All files converted to Parquet format.")

if __name__ == "__main__":
    main()