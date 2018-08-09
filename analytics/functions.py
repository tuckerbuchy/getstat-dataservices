from pyspark.sql.functions import udf, desc, col, to_date, lit
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql import Window
import pyspark.sql.functions as func

from etl.serp import SerpEtl

def most_top_tens(start_date, end_date):
    #TODO: probably dont need the select *
    df = SerpEtl.get_serps()

    function = udf(lambda x: 1 if x <= 10 else 0, IntegerType())
    return df.filter(df['CrawlDate'] >= to_date(lit(start_date)).cast(TimestampType()))\
        .filter(df['CrawlDate'] <= to_date(lit(end_date)).cast(TimestampType()))\
        .select('*', function(col('Rank')).alias('RankedInTen'))\
        .groupBy('URL').agg({'RankedInTen': 'sum'})\
        .withColumnRenamed("sum(RankedInTen)", "TimesInTopTen")\
        .sort(desc("TimesInTopTen"))

def most_rank_1_changes():
    df = SerpEtl.get_serps()
    # TODO: issue when there is multiple of the same keyword in the same day.

    df_new = df.filter(df['Rank'] == 1).select('Keyword', 'CrawlDate', 'URL').withColumnRenamed('URL', 'TopURL').orderBy("Keyword", "CrawlDate")
    w = Window.partitionBy("Keyword").orderBy("CrawlDate")
    df_new = df_new.withColumn("PrevTopURL", func.lag(df_new['TopURL']).over(w))
    return df_new.withColumn("Changed", (df_new['PrevTopURL'] != df_new['TopURL']).cast(IntegerType()))\
        .groupBy("Keyword").agg({"Changed": "sum"}).withColumnRenamed("sum(Changed)", "NumberOfChanges").sort(desc("NumberOfChanges"))

def consistency_across_devices():
    serps = SerpEtl.get_serps()
    serps = serps.fillna('', subset=['location']) # location nulls mess up the join.

    desktop_serps = serps.filter(serps['Device'] == 'desktop')
    mobile_serps = serps.filter(serps['Device'] == 'smartphone')

    # Join on rank and then compare urls from the two devices...
    serps_scored = desktop_serps.alias('desktop')\
        .join(mobile_serps.alias('mobile'),
              (col('desktop.keyword') == col('mobile.keyword')) &
              (col('desktop.market') == col('mobile.market')) &
              (col('desktop.location') == col('mobile.location')) &
              (col('desktop.rank') == col('mobile.rank')))\
        .withColumn("Same", (col('desktop.URL') == col('mobile.URL')).cast(IntegerType()))

    serps_summary = serps_scored.groupBy('desktop.keyword', 'desktop.market', 'desktop.location').agg({'Same': 'sum', 'desktop.keyword': 'count'})
    return serps_summary\
        .withColumn("ConsistencyScore", col("sum(Same)") / col("count(keyword)"))\
        .select('desktop.keyword', 'desktop.market', 'desktop.location', 'ConsistencyScore')\
        .sort(desc("ConsistencyScore"))
