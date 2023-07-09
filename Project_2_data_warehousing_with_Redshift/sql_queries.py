import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Get IAM rolw
IAM_ROLE = config.get("IAM_ROLE", "ARN")
# Get S3 data paths
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
# REGION
REGION = "us-west-2"

# DROP TABLES
staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists songplay;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist         TEXT,
    auth            VARCHAR(20),
    firstName       VARCHAR(50),
    gender          VARCHAR(1),
    itemInSession   INTEGER,
    lastName        VARCHAR(50),
    length          REAL,
    level           VARCHAR(4),
    location        VARCHAR(255),
    method          VARCHAR(5),
    page            VARCHAR(20),
    registration    DOUBLE PRECISION,
    sessionId       INTEGER,
    song            TEXT,
    status          INTEGER,
    ts              BIGINT,
    userAgent       TEXT,
    userId          VARCHAR(20)
);
""")

jls_extract_var = """
CREATE TABLE staging_songs (
    num_songs           INTEGER,
    artist_id           VARCHAR(50),
    artist_latitude     REAL,
    artist_longitude    REAL,
    artist_location     TEXT,
    artist_name         TEXT,
    song_id             VARCHAR(50),
    title               TEXT,
    duration            REAL,
    year                INTEGER
);
"""
staging_songs_table_create = (jls_extract_var)

songplay_table_create = ("""
CREATE TABLE songplay (
    songplay_id     INTEGER NOT NULL PRIMARY KEY, 
    start_time      BIGINT NOT NULL, 
    user_id         VARCHAR(20) NOT NULL, 
    level           VARCHAR(4) NOT NULL, 
    song_id         VARCHAR(50) NOT NULL, 
    artist_id       VARCHAR(50) NOT NULL, 
    session_id      INTEGER NOT NULL, 
    location        TEXT, 
    user_agent      TEXT,
    FOREIGN KEY     (start_time, user_id, song_id, artist_id)
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id     VARCHAR(20) NOT NULL PRIMARY KEY, 
    first_name  VARCHAR(50), 
    last_name   VARCHAR(50), 
    gender      VARCHAR(1), 
    level       VARCHAR(4)
);
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id     VARCHAR(50) NOT NULL PRIMARY KEY, 
    title       VARCHAR(255), 
    artist_id   VARCHAR(50) NOT NULL, 
    year        INTEGER, 
    duration    REAL
);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id   VARCHAR(50) NOT NULL PRIMARY KEY, 
    name        TEXT NOT NULL, 
    location    TEXT, 
    lattitude   REAL, 
    longitude   REAL
);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time BIGINT NOT NULL PRIMARY KEY, 
    hour        INTEGER, 
    day         INTEGER, 
    week        INTEGER, 
    month       INTEGER, 
    year        INTEGER, 
    weekday     INTEGER
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events 
from '{}' 
iam_role '{}'
json '{}';
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs 
from '{}' 
iam_role '{}'
JSON 'auto ignorecase'
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay
(SELECT 
    num_songs AS songplay_id, 
    staging_events.ts AS start_time, 
    staging_events.userId AS user_id, 
    staging_events.level, 
    staging_songs.song_id, 
    staging_songs.artist_id, 
    staging_events.sessionId AS session_id, 
    staging_events.location, 
    REGEXP_SUBSTR(staging_events
.userAgent, 'Mozilla[^ ]+') AS user_agent
FROM staging_events, staging_songs);
""")

user_table_insert = ("""
INSERT INTO users
(SELECT 
    userId AS user_id, 
    firstName AS first_name, 
    lastName AS last_name, 
    gender, 
    level
FROM staging_events
WHERE (userId, firstName, gender, level) IS NOT NULL);
""")

song_table_insert = ("""
INSERT INTO songs
(SELECT 
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
FROM staging_songs
WHERE (song_id, title, artist_id, year, duration) IS NOT NULL);
""")

artist_table_insert = ("""
INSERT INTO artists
(SELECT 
    artist_id, 
    artist_name AS name, 
    artist_location AS location, 
    artist_latitude AS lattitude, 
    artist_longitude AS longitude
FROM staging_songs
WHERE (artist_id, artist_name, artist_location, artist_latitude, artist_longitude) IS NOT NULL);
""")

time_table_insert = ("""
INSERT INTO time
(SELECT 
    ts AS start_time,
    EXTRACT(hour FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS hour,
    EXTRACT(day FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS day,
    EXTRACT(week FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS week,
    EXTRACT(month FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS month,
    EXTRACT(year FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS year,
    EXTRACT(dow FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS weekday
FROM staging_events);
""")

# Query tables

count_stagings_events = ("""
SELECT COUNT(*) FROM staging_events;
""")
count_stagings_songs = ("""
SELECT COUNT(*) FROM staging_songs;
""")
count_songplay = ("""
SELECT COUNT(*) FROM songplay;
""")
count_users = ("""
SELECT COUNT(*) FROM users;
""")
count_songs = ("""
SELECT COUNT(*) FROM songs;
""")
count_artists = ("""
SELECT COUNT(*) FROM artists;
""")
count_time = ("""
SELECT COUNT(*) FROM time;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
count_table_queries = [count_stagings_events, count_stagings_songs, count_songplay, count_users, count_songs, count_artists, count_songs, count_time]

