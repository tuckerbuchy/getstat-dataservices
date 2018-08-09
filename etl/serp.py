from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from spark_utils import get_spark_session

class SerpEtl(object):
    # '/Users/tukrre/Downloads/stat_data/'
    def __init__(self, data_path):
        self.serp_stream = None
        self.data_path = data_path

    @staticmethod
    def get_serps():
        spark = get_spark_session()
        # TODO: un-hardcode paths
        df = spark.read.parquet("/Users/tukrre/Downloads/stat_data/output/").cache()
        return df

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

        # TODO: Custom paths..
        self.serp_stream = spark.readStream.csv(self.data_path, header=True, schema=schema)

    def transform(self):
        self.serp_stream = self.serp_stream.withColumnRenamed("Crawl Date", "CrawlDate")

    def load(self):
        query = self.serp_stream.writeStream.format("parquet")\
            .option("path", "/Users/tukrre/Downloads/stat_data/output/")\
            .option("checkpointLocation", "/Users/tukrre/Downloads/stat_data/checkpoint/").start()
        query.awaitTermination()

    def start(self):
        self.extract()
        self.transform()
        self.load()
