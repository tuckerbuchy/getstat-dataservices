from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from spark_utils import get_spark_session
import os

# SERP_TARGET_DIR, SERP_SOURCE_DIR
class SerpEtl(object):
    output_path = None  # location of the parquet output

    def __init__(self):
        self.serp_stream = None

    @classmethod
    def get_serps(cls):
        spark = get_spark_session()
        parquet_dir = SerpEtl.get_parquet_dir()
        df = spark.read.parquet(parquet_dir).cache()
        return df

    @classmethod
    def get_parquet_dir(cls):
        return os.path.join(os.environ['SERP_TARGET_DIR'], 'parquet')

    @classmethod
    def get_checkpoint_dir(cls):
        return os.path.join(os.environ['SERP_TARGET_DIR'], 'checkpoint')

    def extract(self):
        spark = get_spark_session()
        schema = StructType([
            StructField("Keyword", StringType()),
            StructField("Market", StringType()),
            StructField("Location", StringType()),
            StructField("Device", StringType()),
            StructField("CrawlDate", DateType()),
            StructField("Rank", IntegerType()),
            StructField("URL", StringType())
        ])

        source_dir = os.environ['SERP_SOURCE_DIR']
        self.serp_stream = spark.readStream.csv(source_dir, header=True, schema=schema)

    def transform(self):
        self.serp_stream = self.serp_stream.withColumnRenamed("Crawl Date", "CrawlDate")

    def load(self):
        parquet_dir = SerpEtl.get_parquet_dir()
        checkpoint_dir = SerpEtl.get_checkpoint_dir()
        query = self.serp_stream.writeStream.format("parquet")\
            .option("path", parquet_dir)\
            .option("checkpointLocation", checkpoint_dir).start()
        query.awaitTermination()

    def start(self):
        self.extract()
        self.transform()
        self.load()
