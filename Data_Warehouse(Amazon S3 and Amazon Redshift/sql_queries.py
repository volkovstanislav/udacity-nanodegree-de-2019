import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists song"
artist_table_drop = "drop table if exists artist"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("create table if not exists staging_events (\
                             artist varchar,\
                             auth varchar,\
                             firstname varchar,\
                             gender varchar,\
                             iteminsession int,\
                             lastname varchar,\
                             length float,\
                             level varchar,\
                             location varchar,\
                             method varchar,\
                             page varchar,\
                             registration float,\
                             sessionid int,\
                             song varchar,\
                             status int,\
                             ts varchar,\
                             useragent varchar,\
                             userid varchar )")

staging_songs_table_create = ("create table if not exists staging_songs (\
                              num_songs int,\
                              artist_id varchar,\
                              artist_latitude float,\
                              artist_longitude float,\
                              artist_location varchar,\
                              artist_name varchar,\
                              song_id varchar,\
                              title varchar,\
                              duration float,\
                              year int)")

songplay_table_create = ("create table if not exists songplay (\
                            songplay_id int identity(0,1) primary key, \
                            start_time varchar not null, \
                            user_id int not null, \
                            level varchar , \
                            song_id varchar, \
                            artist_id varchar, \
                            session_id int not null, \
                            location varchar, \
                            user_agent varchar)")

user_table_create = ("create table if not exists users (\
                        user_id int not null primary key, \
                        firstname varchar, \
                        lastname varchar, \
                        gender varchar,\
                        level varchar)")

song_table_create = ("create table if not exists song (\
                        song_id varchar not null primary key, \
                        title varchar, \
                        artist_id varchar, \
                        year int, \
                        duration float)")

artist_table_create = ("create table if not exists artist (\
                          artist_id varchar not null primary key, \
                          name varchar, \
                          location varchar ,\
                          lattitude float, \
                          longitude float)")

time_table_create = ("create table if not exists time (\
                        start_time varchar not null primary key, \
                        hour int, \
                        day int, \
                        week int, \
                        month int, \
                        year int, \
                        weekday int)")

# STAGING TABLES

staging_events_copy = ("copy staging_events from {} \
                     credentials {} \
                     region 'us-west-2' json 'auto';").format("'s3://udacity-dend/log_data/2018/'","'aws_iam_role=arn:aws:iam::317069077155:role/myRedshiftRole'")
  
staging_songs_copy = ("copy staging_songs from {} \
                     credentials {} \
                     region 'us-west-2' json 'auto';").format("'s3://udacity-dend/song-data/A/'","'aws_iam_role=arn:aws:iam::317069077155:role/myRedshiftRole'")


# FINAL TABLES

songplay_table_insert = ("insert into songplay(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) values(%s,%s,%s,%s,%s,%s,%s,%s)")

user_table_insert = ("insert into users values(%s,%s,%s,%s,%s)")

song_table_insert = ("insert into song values(%s,%s,%s,%s,%s)")

artist_table_insert = ("insert into artist values(%s,%s,%s,%s,%s)")

time_table_insert = ("insert into time values(%s,%s,%s,%s,%s,%s,%s)")

# FIND SONGS

song_select = ("select t1.song_id, t2.artist_id from song t1 inner join artist t2 on t1.artist_id = t2.artist_id where t1.title = %s and t2.name = %s and t1.duration = %s")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
