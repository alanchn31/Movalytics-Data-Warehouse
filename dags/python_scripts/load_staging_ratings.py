import sys
import os
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField as Fld, DoubleType as Dbl,
                               IntegerType as Int, TimestampType as Timestamp, 
                               DateType as Date)
from pyspark.sql.functions import (monotonically_increasing_id, col, year, 
                                   month, dayofmonth, udf)


def create_spark_session(aws_key, aws_secret_key):
    """
    Description: Creates spark session.
    Returns:
        spark session object
    """

    spark = SparkSession \
        .builder \
        .config("spark.executor.heartbeatInterval", "40s") \
        .getOrCreate()
    
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl",
                                                      "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "100")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.maximum", "5000")
    return spark


def format_datetime(ts):
    return datetime.fromtimestamp(ts/1000.0) 

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

    ratings_df = spark.read.option("header", "true") \
                           .csv("s3a://{}/{}/ratings.csv".format(s3_bucket, s3_key), 
                                schema=ratings_schema)

    ratings_df = ratings_df.na.drop()

    # get_datetime = udf(lambda x: format_datetime(int(x)), Date())
    # ratings_df = ratings_df.withColumn("datetime", get_datetime(ratings_df.timestamp))
    # ratings_df = ratings_df.filter(ratings_df("datetime") == lit(execution_date))
    # ratings_df = ratings_df.withColumn("year", year(ratings_df.datetime)) \
    #                        .withColumn("month", month(ratings_df.datetime)) \
    #                        .withColumn("day", dayofmonth(ratings_df.datetime))

    ratings_df = ratings_df.select(
        col("userId").alias("user_id"),
        col("movieId").alias("movie_id"),
        col("rating")
    )
    
    output_path = "s3a://{}/{}/{}".format(s3_bucket, "data", "ratings")

    # ratings_df.write.partitionBy("year", "month", "day").parquet(output_path)

    ratings_df.write \
              .format("jdbc")  \
              .option("url", redshift_conn_string) \
              .option("dbtable", "movies.stage_ratings") \
              .option("user", sys.argv[6]) \
              .option("password", sys.argv[7]) \
              .option("driver", "com.amazon.redshift.jdbc42.Driver") \
              .mode("overwrite") \
              .save()
    