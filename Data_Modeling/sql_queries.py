# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("create table if not exists songplays (\
                            songplay_id serial primary key, \
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

song_table_create = ("create table if not exists songs (\
                        song_id varchar not null primary key, \
                        title varchar, \
                        artist_id varchar, \
                        year int, \
                        duration float)")

artist_table_create = ("create table if not exists artists (\
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

# INSERT RECORDS

songplay_table_insert = ("insert into songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) values(%s,%s,%s,%s,%s,%s,%s,%s)")

user_table_insert = ("insert into users values(%s,%s,%s,%s,%s) on conflict (user_id) do update set level = excluded.level where excluded.level='paid'")

song_table_insert = ("insert into songs values(%s,%s,%s,%s,%s) on conflict (song_id) do nothing")

artist_table_insert = ("insert into artists values(%s,%s,%s,%s,%s) on conflict (artist_id) do nothing")

time_table_insert = ("insert into time values(%s,%s,%s,%s,%s,%s,%s) on conflict (start_time) do nothing")

# FIND SONGS

song_select = ("select t1.song_id, t2.artist_id from songs t1 inner join artists t2 on t1.artist_id = t2.artist_id where t1.title = %s and t2.name = %s and t1.duration = %s")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]