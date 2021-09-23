import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Dat, TimestampType
from pyspark.sql.types import LongType
from pyspark.sql.functions import desc, expr, to_timestamp


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    This function creates a spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function would load song data(json) from S3, and processing using spark to create 2 dimension tables by  extracting 
    information from song_data.
        1. songs_table
        2. artist_table
        
    Input: 
    spark : SparkSession
    input_data: input file path (String)
    output_data: output fle path (String)
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"

    songSchema = R([
        Fld('artist_id', Str()),
        Fld('artist_latitude', Dbl()),
        Fld('artist_location', Str()),
        Fld('artist_longitude', Dbl()),
        Fld('artist_name', Str()),
        Fld('duration', Dbl()),
        Fld('num_songs', Int()),
        Fld('song_id', Str()),
        Fld('title', Str()),
        Fld('year', Int())
    ])
    # read song data file
    df = spark.read.json(song_data, schema=songSchema)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data+'songs_table/')

    # extract columns to create artists table
    artist_lst = ['artist_id', \
              'artist_name as name', \
              'artist_location as location', \
              'artist_latitude as lattitude', \
              'artist_longitude as longitude']

    artist_table = df.selectExpr(artist_lst).distinct()
    
    # write artists table to parquet files
    artist_table.write.parquet(output+'artist_table/')


def process_log_data(spark, input_data, output_data):
    
    """
    This function would load song data(json) from S3, and processing using spark to create 2 fact tables and 1 dimension  table by extracting information from log_data and song_data.
        1. users_table (dimension)
        2. time_table (dimension)
        3. song_plays (fact)
        
    Input: 
    spark : SparkSession
    input_data: input file path (String)
    output_data: output fle path (String)
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(col('page') == 'NextSong')

    
    # cast the original ts column to longtype
    df = df.withColumn('ts', expr('CAST(ts AS LONG)'))
    
    # get the most current status as the staus of the user
    df.createOrReplaceTempView('log_data')
    current = spark.sql("""
        SELECT *, 
        ROW_NUMBER() OVER(PARTITION BY userId ORDER BY ts DESC) AS current_status
        FROM log_data
    """)
    
    # extract columns for users table    
    users_table = current.filter(col('current_status')==1)\
                         .select(['userId', 'firstName', 'lastName', 'gender', 'level'])\
                         .distinct()
    
    # write users table to parquet files
    users_table.write.parquet(output+'users_table/')
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(x)/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('datetime', get_datetime(col('ts')))
    
    # extract columns to create time table
    df.createOrReplaceTempView('clean_data')
    time_table = spark.sql("""
        SELECT datetime AS start_time,
        HOUR(datetime) AS hour,
        DAY(datetime) AS day,
        WEEKOFYEAR(datetime) AS week,
        MONTH(datetime) AS month,
        YEAR(datetime) AS year,
        DAYOFWEEK(datetime) AS weekday
        FROM clean_data
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year', 'month']).parquet(output+'time_table/')

    # read in song data to use for songplays table
    songs_table = spark.read.parquet(output_data+'songs_table/')
    artist_table = spark.read.parquet(output_data+'artist_table/')
    

    # extract columns from joined song and log datasets to create songplays table 
    df = df.withColumnRenamed('datetime','start_time')
    songs_play = df.join(songs_table, df.song==songs_table.title, how='inner')\
                .join(artist_table, df.artist==artist_table.name, how='inner')\
                .join(time_table, df.start_time==time_table.start_time, how='inner')\
                .select(monotonically_increasing_id().alias('songplay_id'),\
                        time_table['start_time'], 
                        col('userId'), 
                        col('level'), 
                        col('song_id'), 
                        artist_table['artist_id'],
                        col('sessionId'),
                        df['location'], 
                        col('userAgent'), 
                        time_table['month'], 
                        time_table['year'])
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year','month']).parquet(output+'song_plays/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "./data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
