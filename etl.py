import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    
    """
    - Read the file in the S3
    
    - Create the songs table
    
    - Create the artists table
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        col("song_id").alias("song_id"),
        col("title").alias("title"),
        col("artist_id").alias("artist_id"),
        col("year").alias("year"),
        col("duration").alias("duration")
    )
    
    songs_table = songs_table.drop_duplicates(subset=['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select(
        col("artist_id").alias("artist_id"),
        col("artist_name").alias("name"),
        col("artist_location").alias("location"),
        col("artist_latitude").alias("latitude"),
        col("artist_longitude").alias("longitude")
    )
    
    artists_table = artists_table.drop_duplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')
    

def process_log_data(spark, input_data, output_data):
    
    """
    - Read the file in the S3
    
    - Create the time table
    
    - Create the users table
    
    - Create the songplays table
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # extract columns for users table
    users_table = df.select(
        col("userId").alias("user_id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        col("gender").alias("gender"),
        col("level").alias("level")
    )
    
    users_table = users_table.drop_duplicates(subset=['user_id'])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int((int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("date", get_datetime(df.ts))

    time_table = df.select(
        col('timestamp').alias('start_time'),
        hour('date').alias('hour'),
        dayofmonth('date').alias('day'),
        weekofyear('date').alias('week'),
        month('date').alias('month'),
        year('date').alias('year'),
        date_format('date','E').alias('weekday')
    )
    
    time_table = time_table.drop_duplicates(subset=['start_time'])

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').mode('overwrite').parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(
        song_df, (df.artist == song_df.artist_name) & (df.song == song_df.title)
    ).select(
        col("timestamp").alias("start_time"),
        col("userId").alias("userId"),
        col("level").alias("level"),
        col("song_id").alias("song_id"),
        col("artist_id").alias("artist_id"),
        col("sessionId").alias('sessionId'),
        col("artist_location").alias("artist_location"),
        col("userAgent").alias("user_agent"),
        month('date').alias('month'),
        year('date').alias('year')
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').mode('overwrite').parquet(os.path.join(output_data, 'songplays'))
    

def main():
    
    """
    - Create the spark sessions
    
    - Define the input and output url
    
    - Call the processes to create the tables
    
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
