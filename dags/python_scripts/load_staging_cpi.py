import sys
import os
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField as Fld,
                               DateType as Date, FloatType as Float)
from pyspark.sql.functions import col


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


if __name__ == "__main__":
    s3_bucket = sys.argv[1]
    s3_key = sys.argv[2]
    aws_key = sys.argv[3]
    aws_secret_key = sys.argv[4]
    redshift_conn_string = sys.argv[5]
    db_user = sys.argv[6]
    db_pass = sys.argv[7]

    spark = create_spark_session(aws_key, aws_secret_key)

    cpi_schema = StructType([
        Fld("DATE", Date()),
        Fld("CUSR0000SS62031", Float())
    ])

    cpi_df = spark.read.option("header", "true") \
                           .csv("s3a://{}/{}/consumer_price_index.csv".format(s3_bucket, s3_key), 
                            schema=cpi_schema)

    cpi_df = cpi_df.select(col("DATE").alias("date_cd"),
                           col("CUSR0000SS62031").alias("consumer_price_index"))
    
    cpi_df = cpi_df.filter(cpi_df.date_cd.isNotNull())

    cpi_df.write \
              .format("jdbc")  \
              .option("url", redshift_conn_string) \
              .option("dbtable", "movies.stage_cpi") \
              .option("user", sys.argv[6]) \
              .option("password", sys.argv[7]) \
              .option("driver", "com.amazon.redshift.jdbc42.Driver") \
              .mode("append") \
              .save()