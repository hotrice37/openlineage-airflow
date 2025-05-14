"""
PySpark script to run analysis on Higgs Twitter Parquet datasets.
This script automates the analysis process demonstrated in the ETL-download.ipynb notebook.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, sum

def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Higgs Twitter - Analysis") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()
    
    # Set the HDFS paths for Parquet files
    parquet_dir = "hdfs://hadoop-namenode:8020/HiggsTwitter/parquet/"
    results_dir = "hdfs://hadoop-namenode:8020/HiggsTwitter/results/"
    
    # Load Parquet files into dataframes
    social_df = spark.read.parquet(parquet_dir + "social_network")
    retweet_df = spark.read.parquet(parquet_dir + "retweet_network")
    reply_df = spark.read.parquet(parquet_dir + "reply_network")
    mention_df = spark.read.parquet(parquet_dir + "mention_network")
    activity_df = spark.read.parquet(parquet_dir + "activity_time")
    
    # Register temporary views for SQL queries
    social_df.createOrReplaceTempView("social")
    retweet_df.createOrReplaceTempView("retweet")
    reply_df.createOrReplaceTempView("reply") 
    mention_df.createOrReplaceTempView("mention")
    activity_df.createOrReplaceTempView("activity")
    
    print("Running analysis on Higgs Twitter dataset...")
    
    # Analysis 1: Users with most followers
    print("Analysis 1: Users with most followers")
    top_followed = social_df \
        .groupBy("followed") \
        .agg(count("follower").alias("followers")) \
        .orderBy(desc("followers"))
    
    top_followed.limit(10).show()
    top_followed.write.mode("overwrite").parquet(results_dir + "top_followed")
    
    # Analysis 2: Users with most mentions
    print("Analysis 2: Users with most mentions")
    top_mentioned = mention_df \
        .groupBy("mentioned") \
        .agg(count("occur").alias("mentions")) \
        .orderBy(desc("mentions"))
    
    top_mentioned.limit(10).show()
    top_mentioned.write.mode("overwrite").parquet(results_dir + "top_mentioned")
    
    # Analysis 3: For top 5 followed users, how many mentions they got
    print("Analysis 3: Top 5 followed users with their mention counts")
    top_5_followed = social_df \
        .groupBy("followed") \
        .agg(count("follower").alias("followers")) \
        .orderBy(desc("followers")) \
        .limit(5)
    
    top_followed_mentions = top_5_followed \
        .join(mention_df, top_5_followed["followed"] == mention_df["mentioned"]) \
        .groupBy(top_5_followed["followed"], top_5_followed["followers"]) \
        .agg(sum(mention_df["occur"]).alias("mentions")) \
        .orderBy(desc("followers"))
    
    top_followed_mentions.show()
    top_followed_mentions.write.mode("overwrite").parquet(results_dir + "top_followed_mentions")
    
    # Analysis 4: SQL version - Users with most followers
    print("Analysis 4: SQL version - Users with most followers")
    sql_top_followed = spark.sql("""
        SELECT followed, COUNT(follower) AS followers 
        FROM social 
        GROUP BY followed 
        ORDER BY followers DESC
    """)
    
    sql_top_followed.limit(10).show()
    
    # Analysis 5: SQL version - Top followed users with their mention counts
    print("Analysis 5: SQL version - Top followed users with their mention counts")
    sql_top_followed_mentions = spark.sql("""
        SELECT top_f.followed, top_f.followers, SUM(m.occur) AS mentions
        FROM 
            (SELECT followed, COUNT(follower) AS followers 
             FROM social 
             GROUP BY followed 
             ORDER BY followers DESC 
             LIMIT 5) top_f
        JOIN mention m ON top_f.followed = m.mentioned
        GROUP BY top_f.followed, top_f.followers
        ORDER BY followers DESC
    """)
    
    sql_top_followed_mentions.show()
    
    # Create and save a more detailed report for the top 100 users
    print("Creating detailed report for top 100 users...")
    
    top_100_users = spark.sql("""
        SELECT 
            u.user_id,
            COALESCE(f.followers, 0) AS followers_count,
            COALESCE(m.mentions, 0) AS mentions_count,
            COALESCE(r.replies, 0) AS replies_count,
            COALESCE(rt.retweets, 0) AS retweets_count
        FROM 
            (SELECT followed AS user_id FROM social
             UNION
             SELECT mentioned AS user_id FROM mention
             UNION
             SELECT replied AS user_id FROM reply
             UNION 
             SELECT tweeted AS user_id FROM retweet) u
        LEFT JOIN
            (SELECT followed, COUNT(follower) AS followers FROM social GROUP BY followed) f
            ON u.user_id = f.followed
        LEFT JOIN
            (SELECT mentioned, SUM(occur) AS mentions FROM mention GROUP BY mentioned) m
            ON u.user_id = m.mentioned
        LEFT JOIN
            (SELECT replied, SUM(occur) AS replies FROM reply GROUP BY replied) r
            ON u.user_id = r.replied
        LEFT JOIN
            (SELECT tweeted, SUM(occur) AS retweets FROM retweet GROUP BY tweeted) rt
            ON u.user_id = rt.tweeted
        ORDER BY followers_count DESC, mentions_count DESC
        LIMIT 100
    """)
    
    top_100_users.show(10)
    top_100_users.write.mode("overwrite").parquet(results_dir + "top_100_users")
    
    # Stop the SparkSession
    spark.stop()
    print("Analysis completed. Results saved to HDFS.")

if __name__ == "__main__":
    main()