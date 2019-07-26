import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import functions as F
from pyspark.sql import types as T

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    #song_data = os.path.join(input_data, 'song-data/A/B/N/TRABNQK128F14A2178.json')  
    song_data = os.path.join(input_data, 'song_data/A/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    df.printSchema()

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('artist_id','year').parquet(output_data+'songs_table.parquet')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table.parquet')
    
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    #log_data = os.path.join(input_data,'log_data/2018/11/2018-11-30-events.json')
    log_data = os.path.join(input_data, "log-data/*/*/*.json")

    # read log data file
    print(log_data)
    df = spark.read.json(log_data)
    df.printSchema()
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')
    
    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level')
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    df = df.withColumn('datetime', from_unixtime(F.col('ts')/1000))

    # extract columns to create time table
    time_table = df.select('datetime', \
                           hour('datetime').alias('hour'), \
                           dayofmonth('datetime').alias('day'), \
                           weekofyear('datetime').alias('week'), \
                           month('datetime').alias('month'), \
                           year('datetime').alias('year'), \
                           date_format('timestamp', 'u').alias('weekday'), \
                           'ts')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data+'time_table.parquet')
    
    # read in song data to use for songplays table
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)
    
    song_table = sqlContext.read.parquet(output_data+'songs_table.parquet')
    artists_table = sqlContext.read.parquet(output_data+'artists_table.parquet')
    time_table = sqlContext.read.parquet(output_data+'time_table.parquet')
    
    song_table = song_table.withColumnRenamed("artist_id", "artistId")
    
    condition = [song_table.artistId == artists_table.artist_id]
    songs_artists_table = song_table.join(artists_table,condition)
    
    songs_artists_table.show(2)
    condition = [songs_artists_table.duration == df.length,songs_artists_table.title == df.song,songs_artists_table.artist_name == df.artist]
    song_log_data = songs_artists_table.join(df,condition)
    song_log_data.printSchema()
    
    #condition = [song_log_data.ts == time_table.start_time]
    #song_long_time_data = song_log_data.join(time_table,condition)
    
    # extract columns from joined song and log datasets to create songplays table 

    song_log_data = song_log_data.withColumn('datetime', from_unixtime(F.col('ts')/1000))
    
    songplays_table = song_log_data.select(\
                        monotonically_increasing_id().alias('songplay_id'),\
                        'datetime', \
                        'userId', \
                        'level', \
                        'song_id', \
                        'artist_id', \
                        'sessionId', \
                        'location', \
                        'userAgent',\
                        month('datetime').alias('month'), \
                        year('datetime').alias('year')
                        )
    
    songplays_table.printSchema()
    songplays_table.show(2)
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data+'songplays_table.parquet')


def test (spark, output_data):
    #read data from parquet format
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)
    
    song_table = sqlContext.read.parquet(output_data+'songs_table.parquet')
    artists_table = sqlContext.read.parquet(output_data+'artists_table.parquet')
    time_table = sqlContext.read.parquet(output_data+'time_table.parquet')
    users_table = sqlContext.read.parquet(output_data+'users_table.parquet')
    songplays_table = sqlContext.read.parquet(output_data+'songplays_table.parquet')
    
    # How did the average duration of the application's songs change over the years
    song_table.groupby('year').avg('duration').sort('year', ascending=False).show()
    
    # How many unique artists of the applications data
    print("unique artists of the applications data - " + str(artists_table.select('artist_id').dropDuplicates().count()))
    
    # Distribution by gender of application users
    users_table.groupby('gender').count().show()
    
    # Most women popular firstname of application user
    print(users_table.filter(users_table.gender == 'F').groupby('firstName').count().sort('count', ascending=False).take(1))
    
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)        
    process_log_data(spark, input_data, output_data)

    test(spark, output_data)

if __name__ == "__main__":
    main()
