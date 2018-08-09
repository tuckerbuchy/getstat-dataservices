from pyspark.sql import SparkSession

def get_spark_session():
    ss = SparkSession \
        .builder \
        .master("local[*]") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.0' ) \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.coll") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.coll") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .appName("StatApp") \
        .getOrCreate()

    return ss

def get_logger():
    spark = get_spark_session()
    log4jLogger = spark._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    LOGGER.info("pyspark script logger initialized")
    return LOGGER