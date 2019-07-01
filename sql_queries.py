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
start_time TIMESTAMP NOT NULL,
user_id TEXT NOT NULL,
level TEXT,
song_id TEXT NOT NULL,
artist_id TEXT NOT NULL,
session_id INT,
location TEXT,
user_agent TEXT
);
""")

user_table_create = ("""
CREATE TABLE users (
user_id TEXT PRIMARY KEY,
first_name TEXT,
last_name TEXT,
gender TEXT,
level TEXT
);
""")

song_table_create = ("""
CREATE TABLE songs (
song_id TEXT PRIMARY KEY,
title TEXT,
artist_id TEXT NOT NULL,
year INT,
duration DECIMAL
);
""")

artist_table_create = ("""
CREATE TABLE artists (
artist_id TEXT PRIMARY KEY,
name TEXT,
location TEXT,
latitude TEXT,
longitude TEXT
);
""")

time_table_create = ("""
CREATE TABLE time (
start_time TIMESTAMP PRIMARY KEY,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday INT
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
SELECT DISTINCT
TIMESTAMP 'epoch' + ts/1000 * interval '1 second',
userid,
level,
songid,
artist_id,
sessionid,
location,
useragent
FROM staging_events e
LEFT JOIN staging_songs s on (e.song = s.title and e.artist = s.artist_name and e.length = s.duration)
WHERE page = 'NextSong'
AND userid IS NOT NULL
ON CONFLICT (user_id)
DO UPDATE SET level = EXCLUDED.level;
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
userid,
firstname,
lastname,
gender,
level
FROM staging_events
WHERE userid IS NOT NULL
AND page = 'NextSong'
ON CONFLICT (user_id)
DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
songid,
title,
artist_id,
year,
duration
FROM staging_songs
WHERE songid IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
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
SELECT DISTINCT
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
