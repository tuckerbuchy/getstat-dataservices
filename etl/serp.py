from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from spark_utils import get_spark_session
import os

class SerpEtl(object):
    """
    A SERP ETL class to provide functionality to extract, transform, load SERP CSV data.

    This class works by starting a stream on a provided directory given by $SERP_SOURCE_DIR which
    streams new CSV files and writes them to the more efficient Parquet datastore, located at
    $SERP_TARGET_DIR.

    """
    def __init__(self):
        self.serp_stream = None

    @classmethod
    def get_serps(cls):
        """
        Get the Dataframe containing all SERPs.

        :return: A dataframe of SERPs.
        """
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
        """
        Opens a stream of the CSV files from the SERP_SOURCE_DIR.
        """
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
        """
        Makes changes to the SERP Dataframe to allow it to be loaded downstream.
        """
        self.serp_stream = self.serp_stream.withColumnRenamed("Crawl Date", "CrawlDate")

    def load(self):
        """
        Grabs the stream opened in extract and pipes the transformed data to a Parquet datastore.

        This function runs an infinite loop.
        """
        parquet_dir = SerpEtl.get_parquet_dir()
        checkpoint_dir = SerpEtl.get_checkpoint_dir()
        query = self.serp_stream.writeStream.format("parquet")\
            .option("path", parquet_dir)\
            .option("checkpointLocation", checkpoint_dir).start()
        query.awaitTermination()

    def start(self):
        """
        Start the ETL pipeline.
        """
        self.extract()
        self.transform()
        self.load()
