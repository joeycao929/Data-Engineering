import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

log_data = config.get('S3', 'LOG_DATA')
log_jsonpath = config.get('S3', 'LOG_JSONPATH')
song_data = config.get('S3', 'SONG_DATA')
arn = config.get('IAM_ROLE', 'ARN')

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
    CREATE TABLE IF NOT EXISTS staging_events(
        artist          VARCHAR,
        auth            VARCHAR,
        firstname       VARCHAR,
        gender          VARCHAR,
        itemInSesion    INTEGER,
        lastname        VARCHAR,
        length          FLOAT,
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR(4),
        page            VARCHAR,
        registration    VARCHAR,
        sessionId       INTEGER,
        song            VARCHAR,
        status          INTEGER,
        ts              TIMESTAMP,
        userAgent       VARCHAR,
        userId          INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs          INTEGER,
        artist_id          VARCHAR,
        artist_latitude    FLOAT,
        artist_longitude   FLOAT,
        artist_location    VARCHAR,
        artist_name        VARCHAR,
        song_id            VARCHAR,
        title              VARCHAR,
        duration           FLOAT,
        year               INTEGER
        );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id   INTEGER      IDENTITY(0,1) PRIMARY KEY,
        start_time    TIMESTAMP    NOT NULL REFERENCES time(start_time) sortkey,
        user_id       INTEGER      NOT NULL REFERENCES users(user_id) distkey,
        level         VARCHAR(10)  NOT NULL,
        song_id       VARCHAR(100) NOT NULL REFERENCES songs(song_id),
        artist_id     VARCHAR(50)  NOT NULL REFERENCES artists(artist_id),
        sessionId     INTEGER      NOT NULL,
        location      VARCHAR(100) ,
        user_agent    VARCHAR      NOT NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id      INTEGER      PRIMARY KEY DISTKEY,
        first_name   VARCHAR(20)  NOT NULL,
        last_name    VARCHAR(20)  NOT NULL,
        gender       VARCHAR(1),
        level        VARCHAR      NOT NULL 
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id      VARCHAR      PRIMARY KEY DISTKEY,
        title        VARCHAR      NOT NULL,
        artist_id    VARCHAR      NOT NULL,
        year         INTEGER,
        duration     FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id    VARCHAR      PRIMARY KEY DISTKEY,
        name         VARCHAR      NOT NULL,
        location     VARCHAR,
        latitude    DECIMAL(15, 5),
        longitude    DECIMAL(15, 5)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time  TIMESTAMP  PRIMARY KEY SORTKEY DISTKEY,
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
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as JSON {}
    compupdate off region 'us-west-2'
    timeformat as 'epochmillisecs'
    truncatecolumns blanksasnull emptyasnull
""").format(log_data, arn, log_jsonpath)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    format as JSON 'auto'
    compupdate off region 'us-west-2'
    truncatecolumns blanksasnull emptyasnull
""").format(song_data, arn)

# FINAL TABLES


user_table_insert = ("""
    INSERT INTO users 
    (user_id, first_name, last_name, gender, level)
    SELECT userId            AS user_id, 
           firstname         AS first_name, 
           lastname          AS last_name, 
           gender,
           level
    FROM staging_events
    WHERE userId NOT IN (SELECT DISTINCT user_id FROM users) AND page = 'NextSong';
""")


song_table_insert = ("""
    INSERT INTO songs
    (song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id), 
           title, 
           artist_id, 
           year, 
           duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists
    (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id), 
           artist_name        AS name, 
           artist_location    AS location, 
           artist_latitude    AS latitude, 
           artist_longitude   AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
    
""")

time_table_insert = ("""
    INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT(ts)           AS start_time,
           EXTRACT(hour FROM ts)  AS hour,
           EXTRACT(day FROM ts)   AS day,
           EXTRACT(week FROM ts)  AS week,
           EXTRACT(month FROM ts) AS month,
           EXTRACT(year FROM ts)  AS year,
           EXTRACT(weekday FROM ts) AS weekday
    FROM staging_events
    WHERE ts IS NOT NULL;
""")


songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, 
                          level, song_id, artist_id, 
                          sessionId, location, user_agent)
    SELECT e.ts                 AS start_time, 
           e.userId             AS user_id,
           e.level,
           s.song_id, 
           s.artist_id, 
           e.sessionId,
           s.artist_location    AS location, 
           e.userAgent          AS userAgent
    FROM staging_events e
    JOIN staging_songs s ON e.artist = s.artist_name AND s.title = e.song
    WHERE page = 'NextSong';
""")

# Count rows
staging_events_counts = ("""
    SELECT COUNT(*)
    FROM staging_events;
""")

staging_songs_counts = ("""
    SELECT COUNT(*)
    FROM staging_songs;
""")

songplays_counts = ("""
    SELECT COUNT(*)
    FROM songplays;
""")

user_counts = ("""
    SELECT COUNT(*)
    FROM users;
""")

artist_counts = ("""
    SELECT COUNT(*)
    FROM artists;
""")

song_counts = ("""
    SELECT COUNT(*)
    FROM songs;
""")

time_counts = ("""
    SELECT COUNT(*)
    FROM time;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
count_table_entries = [staging_events_counts, staging_songs_counts, songplays_counts, user_counts, artist_counts, song_counts, time_counts]