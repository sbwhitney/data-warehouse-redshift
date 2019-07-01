import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE staging_events (
artist TEXT,
auth TEXT,
firstname TEXT,
gender TEXT,
iteminsession TEXT,
lastname TEXT,
length TEXT,
level TEXT,
location TEXT,
method TEXT,
page TEXT,
registration DECIMAL,
sessionid INT,
song TEXT,
status INT,
ts BIGINT,
useragent TEXT,
userid TEXT
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
num_songs INT,
artist_id TEXT,
artist_latitude TEXT,
artist_longitude TEXT,
artist_location TEXT,
artist_name TEXT,
songid TEXT,
title TEXT,
duration DECIMAL,
year INT
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
songplay_id INT IDENTITY(0,1) PRIMARY KEY,
start_time TIMESTAMP,
user_id TEXT NOT NULL,
level TEXT NOT NULL,
song_id TEXT NOT NULL,
artist_id TEXT NOT NULL,
session_id INT NOT NULL,
location TEXT NOT NULL,
user_agent TEXT NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE users (
user_id TEXT PRIMARY KEY,
first_name TEXT NOT NULL,
last_name TEXT NOT NULL,
gender TEXT,
level TEXT NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE songs (
song_id TEXT PRIMARY KEY,
title TEXT NOT NULL,
artist_id TEXT NOT NULL,
year INT,
duration DECIMAL NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE artists (
artist_id TEXT PRIMARY KEY,
name TEXT NOT NULL,
location TEXT NOT NULL,
latitude TEXT,
longitude TEXT
);
""")

time_table_create = ("""
CREATE TABLE time (
start_time TIMESTAMP PRIMARY KEY,
hour INT NOT NULL,
day INT NOT NULL,
week INT NOT NULL,
month INT NOT NULL,
year INT NOT NULL,
weekday INT NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {0} credentials 'aws_iam_role={1}' compupdate off region 'us-west-2' format as json 'auto';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
COPY staging_songs FROM {0} credentials 'aws_iam_role={1}' compupdate off region 'us-west-2' format as json 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
TIMESTAMP 'epoch' + ts/1000 * interval '1 second',
userid,
level,
songid,
artist_id,
sessionid,
location,
useragent
FROM staging_events e
LEFT JOIN staging_songs s on (song = title)
WHERE page = 'NextSong'
AND userid IS NOT NULL;
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT
userid,
firstname,
lastname,
gender,
level
FROM staging_events
WHERE userid IS NOT NULL;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT
songid,
title,
artist_id,
year,
duration
FROM staging_songs
WHERE songid IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT
artist_id,
artist_name,
artist_location,
artist_latitude,
artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
TIMESTAMP 'epoch' + ts/1000 * interval '1 second',
EXTRACT(hour FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second'),
EXTRACT(day FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second'),
EXTRACT(week FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second'),
EXTRACT(month FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second'),
EXTRACT(year FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second'),
EXTRACT(dow FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second')
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
