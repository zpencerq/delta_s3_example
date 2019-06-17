# delta_s3_example
Simple example of Delta S3 write to mocked S3 and errors

This boots up a mocked S3 server using `moto` and creates a SparkSession pointing at that `moto_server` to demonstrate writing using delta format.

Just run `./run.sh`.
