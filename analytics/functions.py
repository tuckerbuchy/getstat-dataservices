from pyspark.sql.functions import udf, desc, col, to_date, lit
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql import Window
import pyspark.sql.functions as func

from etl.serp import SerpEtl

def most_top_tens(start_date, end_date):
    """
    Answers the question:

    Which URL has the most ranks in the top 10 across all keywords over the period?

    :param start_date: The start date to query from (yyyy-MM-dd)
    :param end_date: The end date to query to (yyyy-MM-dd)
    :return: A dataframe sorted by the URL most often in the top ten.
    """
    df = SerpEtl.get_serps()

    # Filter out for Crawl date band.
    df = df.filter(df['CrawlDate'] >= to_date(lit(start_date)).cast(TimestampType()))\
        .filter(df['CrawlDate'] <= to_date(lit(end_date)).cast(TimestampType()))

    # Create a UDF that marks 1 if the URL was in the top ten.
    function = udf(lambda x: 1 if x <= 10 else 0, IntegerType())

    # Create a new column with the UDF.
    df = df.select('URL', function(col('Rank')).alias('RankedInTen'))

    # Perform the aggregation to get the top URLs.
    return df.groupBy('URL').agg({'RankedInTen': 'sum'})\
        .withColumnRenamed("sum(RankedInTen)", "TimesInTopTen")\
        .sort(desc("TimesInTopTen"))

def most_rank_1_changes(start_date, end_date):
    """
    Answers the question:

    Provide the set of keywords (keyword information) where the rank 1 URL changes the most over
    the period. A change, for the purpose of this question, is when a given keyword's rank 1 URL is
    different from the previous day's URL.

    :return: A dataframe sorted by the URL most often in the top ten.
    """
    df = SerpEtl.get_serps()


    # Extract URLs ranked at number 1, as that is all we are concerned about.
    df = df.filter(df['Rank'] == 1).select('Keyword', 'CrawlDate', 'URL').withColumnRenamed('URL', 'TopURL').orderBy("Keyword", "CrawlDate")

    # Drop duplicates as we only want 1 SERP per keyword per day.
    df = df.drop_duplicates(['Keyword', 'CrawlDate'])

    # Partition the DF by keyword, as we want to find change over time based on keyword.
    w = Window.partitionBy("Keyword").orderBy("CrawlDate")

    # Add a column containing the previous days top URL.
    df = df.withColumn("PrevTopURL", func.lag(df['TopURL']).over(w))

    # Sum the number of changes.
    return df.withColumn("Changed", (df['PrevTopURL'] != df['TopURL']).cast(IntegerType()))\
        .groupBy("Keyword").agg({"Changed": "sum"}).withColumnRenamed("sum(Changed)", "NumberOfChanges").sort(desc("NumberOfChanges"))

def consistency_across_devices():
    """
    Answers the question:

    We would like to understand how similar the results returned for the same keyword, market, and
    location are across devices. For the set of keywords, markets, and locations that have data for
    both desktop and smartphone devices, please devise a measure of difference to indicate how
    similar the URLs and ranks are, and please use this to show how the mobile and desktop results
    in our provided data set converge or diverge over the period.

    Creates a dataframe with the column "ConsistencyScore".

    ConsistencyScore is a measure of how different Rank/URLs are between devices,
    for the same Keyword/Market/Location combination. When a Rank/URL is the same
    between devices, we give that a 1. When they are different, we give a 0.

    The score is the summed values of these differences divided by the total number of
    Ranks/URLs.

    :return: A dataframe sorted by ConsistencyScore.
    """

    # TODO: What if there is no smartphone for a desktop? May need to filter these out.
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
