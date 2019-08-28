create_artists_table = """
    CREATE TABLE if not exists public.artists (
	    artistid varchar(256) NOT NULL,
	    name varchar(256),
	    location varchar(256),
	    lattitude numeric(18,0),
	    longitude numeric(18,0));
    """

create_songplays_table = """
    CREATE TABLE if not exists public.songplays (
	    playid varchar(32) NOT NULL,
	    start_time timestamp NOT NULL,
	    userid int4 NOT NULL,
	    "level" varchar(256),
	    songid varchar(256),
	    artistid varchar(256),
	    sessionid int4,
	    location varchar(256),
	    user_agent varchar(256),
	    CONSTRAINT songplays_pkey PRIMARY KEY (playid));
    """

create_songs_table = """
    CREATE TABLE if not exists public.songs (
	    songid varchar(256) NOT NULL,
	    title varchar(256),
	    artistid varchar(256),
	    "year" int4,
	    duration numeric(18,0),
	    CONSTRAINT songs_pkey PRIMARY KEY (songid));
    """

create_staging_events_table = """
    CREATE TABLE if not exists public.staging_events (
	    artist varchar(256),
	    auth varchar(256),
	    firstname varchar(256),
	    gender varchar(256),
	    iteminsession int4,
	    lastname varchar(256),
	    length numeric(18,0),
	    "level" varchar(256),
	    location varchar(256),
	    "method" varchar(256),
	    page varchar(256),
	    registration numeric(18,0),
	    sessionid int4,
	    song varchar(256),
	    status int4,
	    ts int8,
	    useragent varchar(256),
	    userid int4);
    """

create_staging_songs_table = """
    CREATE TABLE if not exists public.staging_songs (
	    num_songs int4,
	    artist_id varchar(256),
	    artist_name varchar(256),
	    artist_latitude numeric(18,0),
	    artist_longitude numeric(18,0),
	    artist_location varchar(256),
	    song_id varchar(256),
	    title varchar(256),
	    duration numeric(18,0),
	    "year" int4)
    """
create_users_table = """
    CREATE TABLE if not exists public.users (
	    userid int4 NOT NULL,
	    first_name varchar(256),
	    last_name varchar(256),
	    gender varchar(256),
	    "level" varchar(256),
	    CONSTRAINT users_pkey PRIMARY KEY (userid))
    """

create_time_table = """
    CREATE TABLE if not exists public.time (
        start_time varchar not null primary key,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int);
    """

copy_from_s3 = """
    copy {}
    from '{}'
    credentials 'aws_access_key_id={};aws_secret_access_key={}' 
    region 'us-west-2'
    compupdate off
    json '{}';
    """




