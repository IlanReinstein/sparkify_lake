import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Read all JSON files related to the songs in the Sparkify app 
    to create dimensional tables for the data lake.

    Read JSON format and store produced tables in Parquet formt in the 
    specified output directory.

    Parameters:
    ==================
    spark: Running/Active Spark session
    input_data: S3 bucket or local directory to the JSON files
    output_data: S3 bucket or local directory to write parquet files

    Returns: None
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.dropDuplicates('song_id').select(['song_id', 'title', 'artist_id', 'year', 'duration']).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs.parquet"))

    # extract columns to create artists table
    artists_table = df.dropDuplicates('artist_id').selectExpr("artist_id", "artist_name as name", "artist_location AS location",
                    "artist_latitude AS latitude", "artist_longitude AS logitude").distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists.parquet"))


def process_log_data(spark, input_data, output_data):

    """
    Read all JSON files related to the events data in the Sparkify app 
    to create dimensional tables for the data lake.

    Read JSON format and store produced tables in Parquet formt in the 
    specified output directory.

    Parameters:
    ==================
    spark: Running/Active Spark session
    input_data: S3 bucket or local directory to the JSON files
    output_data: S3 bucket or local directory to write parquet files

    Returns: None
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")
    df.createOrReplaceTempView("log_data")

    # extract columns for users table    
    users_table = df..dropDuplicates('user_id')select(['userId', 'firstName', 'lastName', 'gender', 'level']).distinct()

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users.parquet"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn('date', to_date(get_datetime(df.ts)))
    
    # extract columns to create time table
    time_table = df.dropDuplicates('start_time').select(col("start_time"),
                hour(col("start_time")).alias("hour"), 
                dayofmonth(col("start_time")).alias("day"), 
                weekofyear(col("start_time")).alias("week"),
                month(col("start_time")).alias("month"), 
                year(col("start_time")).alias("year"),
                dayofweek(col("start_time")).alias("dow"))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month")parquet(output_data + "timetable")

    # read in song data to use for songplays table
    song_path = os.path.join(input_data, 'song_data/*/*/*/*.json')
    song_df = spark.read.json(song_path)
    song_df.createOrReplaceTempView("song_data")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT log_data.start_time, log_data.userid, log_data.level, 
        song_data.song_id, song_data.artist_id,
        log_data.sessionid, log_data.location, log_data.useragent,
        year(log_data.start_time) as year, month(log_data.start_time) as month
        FROM log_data JOIN song_data
        ON song_data.title = log_data.song
        AND song_data.artist_name = log_data.artist
        WHERE log_data.page = 'NextSong'
        """
        )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_path, "songplays"))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://aws-emr-resources-722777237427-us-east-1/sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
