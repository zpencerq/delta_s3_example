import boto3

from pyspark import SparkConf
from pyspark.sql import SparkSession

def run(spark):
    s3 = boto3.resource("s3", endpoint_url=f"http://localhost:5000")
    s3.create_bucket(Bucket='test-bucket')

    # Works just fine
    (
        spark.createDataFrame([(1, 2), (2, 3)], ['a', 'b'])
        .write
        .format("parquet")
        .save("s3a://test-bucket/foo/parquet")
    )

    # Fails
    (
        spark.createDataFrame([(1, 2), (2, 3)], ['a', 'b'])
        .write
        .format("delta")
        .save("s3a://test-bucket/foo/delta")
    )

if __name__ == '__main__':
    session = (
        SparkSession
        .builder
        .appName('delta_s3_example')
        .config("spark.jars.packages", "io.delta:delta-core_2.11:0.1.0,org.apache.hadoop:hadoop-aws:2.7.3,com.amazonaws:aws-java-sdk:1.7.4")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", "mock")
        .config("spark.hadoop.fs.s3a.secret.key", "mock")
        .config("spark.hadoop.fs.s3a.endpoint", 'http://127.0.0.1:5000')
        .getOrCreate()
    )

    run(session)

    session.stop()
