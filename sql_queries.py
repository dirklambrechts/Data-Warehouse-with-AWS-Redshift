import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                    CREATE TABLE IF NOT EXISTS staging_events (
                        artist TEXT, 
                        auth TEXT,
                        firstName TEXT,
                        gender TEXT, 
                        itemInSession INT, 
                        lastName TEXT, 
                        length FLOAT,
                        level TEXT, 
                        location TEXT, 
                        method TEXT, 
                        page TEXT, 
                        registration FLOAT, 
                        sessionId INT, 
                        song TEXT, 
                        status INT, 
                        ts BIGINT, 
                        userAgent TEXT, 
                        userId INT
                        );
""")

staging_songs_table_create = ("""
                    CREATE TABLE IF NOT EXISTS staging_songs (
                        artist_id TEXT,
                        artist_latitude FLOAT,
                        artist_location TEXT,
                        artist_longitude FLOAT,
                        artist_name TEXT,
                        duration FLOAT,
                        num_songs INT,
                        song_id TEXT,
                        title TEXT,
                        year INT
                        );
""")

songplay_table_create = ("""
                    CREATE TABLE IF NOT EXISTS songplays (
                        songplay_id INT IDENTITY(1,1) PRIMARY KEY, 
                        start_time TIMESTAMP NOT NULL SORTKEY, 
                        user_id INT NOT NULL DISTKEY, 
                        level VARCHAR, 
                        song_id VARCHAR, 
                        artist_id VARCHAR, 
                        session_id INT, 
                        location VARCHAR, 
                        user_agent VARCHAR
                        ) diststyle key;
""")

user_table_create = ("""
                    CREATE TABLE IF NOT EXISTS users (
                        user_id TEXT PRIMARY KEY SORTKEY, 
                        first_name VARCHAR, 
                        last_name VARCHAR, 
                        gender VARCHAR, 
                        level VARCHAR
                        ) diststyle all;
""")

song_table_create = ("""
                    CREATE TABLE IF NOT EXISTS songs (
                        song_id VARCHAR PRIMARY KEY SORTKEY, 
                        title VARCHAR, 
                        artist_id VARCHAR, 
                        year INT, 
                        duration FLOAT
                        ) diststyle all;
""")

artist_table_create = ("""
                    CREATE TABLE IF NOT EXISTS artists (
                        artist_id VARCHAR PRIMARY KEY SORTKEY, 
                        name VARCHAR, 
                        location VARCHAR, 
                        latitude FLOAT, 
                        longitude FLOAT
                        ) diststyle all;
""")

time_table_create = ("""
                    CREATE TABLE IF NOT EXISTS time (
                        start_time TIMESTAMP PRIMARY KEY SORTKEY, 
                        hour INT, 
                        day INT, 
                        week INT, 
                        month INT, 
                        year INT, 
                        weekday INT
                        ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY {} 
    FROM {} 
    IAM_ROLE '{}'
    JSON {} 
    REGION '{}';
    """).format(
        'staging_events', 
        config.get('S3','LOG_DATA'), 
        config.get('IAM_ROLE','ARN'), 
        config.get('S3', 'LOG_JSONPATH'),
        config.get('DWH', 'DWH_REGION')
        )

staging_songs_copy = ("""
    COPY {} 
    FROM {}
    IAM_ROLE '{}'
    JSON 'auto'
    REGION '{}';
    """).format(
        'staging_songs', 
        config.get('S3', 'SONG_DATA'),
        config.get('IAM_ROLE', 'ARN'),
        config.get('DWH', 'DWH_REGION')
        )

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        TIMESTAMP 'epoch' + (staging_events.ts/1000 * INTERVAL '1 second') AS start_time, 
        staging_events.userid, 
        staging_events.level, 
        staging_songs.song_id, 
        staging_songs.artist_id, 
        staging_events.sessionid, 
        staging_events.location, 
        staging_events.useragent
    FROM staging_events 
    LEFT JOIN staging_songs
    ON staging_events.song = staging_songs.title
        AND staging_events.artist = staging_songs.artist_name
    WHERE staging_events.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userid, 
        firstname, 
        lastname, 
        gender, 
        level
    FROM staging_events
    WHERE userid IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
    FROM staging_songs WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude
    FROM staging_songs WHERE artist_id IS NOT NULL
""")


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT 
            start_time, 
            EXTRACT(hour FROM start_time), 
            EXTRACT(day FROM start_time), 
            EXTRACT(week FROM start_time), 
            EXTRACT(month FROM start_time),
            EXTRACT(year FROM start_time), 
            EXTRACT(week FROM start_time)
        FROM songplays 
""")
                  
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
