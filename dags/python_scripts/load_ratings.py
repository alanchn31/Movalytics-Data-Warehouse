# import boto3
import sys
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField as Fld, DoubleType as Dbl,
                               IntegerType as Int, TimestampType as Timestamp)
from pyspark.sql.functions import monotonically_increasing_id, col


def create_spark_session(aws_key, aws_secret_key):
    """
    Description: Creates spark session.
    Returns:
        spark session object
    """

    spark = SparkSession \
        .builder \
        .getOrCreate()
        # .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
        # .config("spark.jars.packages", "com.amazonaws:aws-java-sdk:1.7.4") \
        # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
    
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl",
                                                      "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.multipart.size", "104857600")
    return spark

if __name__ == "__main__":
    s3_bucket = sys.argv[1]
    s3_key = sys.argv[2]
    aws_key = sys.argv[3]
    aws_secret_key = sys.argv[4]
    redshift_conn_string = sys.argv[5]
    db_user = sys.argv[6]
    db_pass = sys.argv[7]

    spark = create_spark_session(aws_key, aws_secret_key)

    ratings_schema = StructType([
        Fld("userId", Int()),
        Fld("movieId", Int()),
        Fld("rating", Dbl()),
        Fld("timestamp", Timestamp())
    ])

    ratings_df = spark.read.csv("s3a://{}/{}/ratings.csv".format(s3_bucket, s3_key), 
                                schema=ratings_schema)
    ratings_df = ratings_df.withColumn("userMovieId", monotonically_increasing_id())
    ratings_df = ratings_df.select(col("userMovieId").alias("user_movie_id"),
                                   col("userId").alias("user_id"),
                                   col("movieId").alias("movie_id"),
                                   col("rating").alias("movie_rating"))
    ratings_df.write \
              .format("jdbc")  \
              .option("url", redshift_conn_string) \
              .option("dbtable", "movies.ratings") \
              .option("user", sys.argv[6]) \
              .option("password", sys.argv[7]) \
              .option("driver", "org.postgresql.Driver") \
              .mode("append") \
              .save()
            #   .option("aws_region", "us-west-2") \
            #   .option("tempdir", "s3a://{}/{}".format(s3_bucket, 'tmp')) \
    