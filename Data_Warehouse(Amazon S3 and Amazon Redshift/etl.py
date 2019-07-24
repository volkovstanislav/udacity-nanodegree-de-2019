import configparser
import psycopg2
import datetime
from sql_queries import copy_table_queries, insert_table_queries, song_select, drop_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e)

def insert_tables(cur, conn):

    # insert into song table
    try:
        query_from_songs = "select song_id, title, artist_id, year, duration from staging_songs"
        cur.execute(query_from_songs)
        conn.commit()
        records_from_songs = cur.fetchall()
    except psycopg2.Error as e:
        print(e) 
   
    query = insert_table_queries[2]
    
    for row in records_from_songs:
        try:
            cur.execute(query,(row[0], row[1], row[2], int(row[3]), float(row[4]) if row[4] is not None else 0))
            conn.commit()
        except psycopg2.Error as e:
            print(e)

    #insert into artist table
    try:
        query_from_songs = "select artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs"
        cur.execute(query_from_songs)
        conn.commit()
        records_from_songs = cur.fetchall()
    except psycopg2.Error as e:
        print(e)
        
    query = insert_table_queries[3]
    
    for row in records_from_songs:
        try:
            cur.execute(query,(row[0], row[1], row[2], float(row[3]) if row[3] is not None else 0, float(row[4]) if row[4] is not None else 0))
            conn.commit()
        except psycopg2.Error as e:
            print(e)
    
    #insert into user table
    try:
        query_from_events = "select userId, firstName, lastName, gender, level from staging_events where page = 'NextSong'"
        cur.execute(query_from_events)
        conn.commit()
        records_from_events = cur.fetchall()
    except psycopg2.Error as e:
        print(e)
        
    query = insert_table_queries[1]
    
    for row in records_from_events:
        try:
            cur.execute(query,(int(row[0]) if row[0] is not None else 0, row[1], row[2], row[3], row[4]))
            conn.commit()            
        except psycopg2.Error as e:
            print(e)
            
    #insert into time table
    try:
        query_from_events = "select ts from staging_events where page = 'NextSong'"
        cur.execute(query_from_events)
        conn.commit()
        records_from_events = cur.fetchall()
    except psycopg2.Error as e:
        print(e)
        
    query = insert_table_queries[4]  
    
    for row in records_from_events:
        try:
            t = datetime.datetime.fromtimestamp(int(row[0])/1000)
            cur.execute(query,(row[0], t.hour, t.day, t.isocalendar()[1], t.month, t.year, t.weekday()))
            conn.commit() 
        except psycopg2.Error as e:
            print(e)
            
    #insert into songplay table
    try:
        query_from_events = "select * from staging_events where page = 'NextSong'"
        cur.execute(query_from_events)
        conn.commit()
        records_from_events = cur.fetchall()
    except psycopg2.Error as e:
        print(e)    
    
    for row in records_from_events:    
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row[13], row[0], row[6]))
        results = cur.fetchone()
    
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        query = insert_table_queries[0]
        if not ((row[17] is None) or (row[15] is None)):
            songplay_data = (row[15], row[17], row[7], songid, artistid, row[12], row[8], row[16])
            cur.execute(query, songplay_data)
        
def drop_tables(cur,conn):
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e)
            
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    drop_tables(cur,conn)

    conn.close()


if __name__ == "__main__":
    main()