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
        CREATE TABLE staging_events(
            artist              text distkey,
            auth                text,
            firstName           text,
            gender              text,
            itemInSession       int,
            lastName            text,
            length              numeric,
            level               text,
            location            text,
            method              text,
            page                text,
            registration        numeric,
            sessionId           int,
            song                text,
            status              int,
            ts                  timestamp,
            userAgent           text,
            userId              int
        )
""")

staging_songs_table_create = ("""
        CREATE TABLE staging_songs(
            artist_id           text,
            artist_latitude     float,
            artist_location     varchar(max),
            artist_longitude    float,
            artist_name         varchar(max) distkey,
            duration            numeric,
            num_songs           int,
            song_id             text,
            title               varchar(max),
            year                int
        )
""")

songplay_table_create = (""" 
       CREATE TABLE songplays(
            songplay_id         int             IDENTITY (0,1) PRIMARY KEY,
            start_time          timestamp       NOT NULL sortkey,
            user_id             varchar         NOT NULL,
            level               varchar,
            song_id             varchar         NOT NULL,
            artist_id           varchar         NOT NULL distkey,
            session_id          int,
            location            varchar,
            user_agent          varchar
        )
""")

user_table_create = ("""
        CREATE TABLE users(
            user_id             int             PRIMARY KEY,
            first_name          varchar,
            last_name           varchar,
            gender              varchar,
            level               varchar         sortkey
        )
""")

song_table_create = ("""
        CREATE TABLE songs(
            song_id             varchar         PRIMARY KEY,
            title               varchar,
            artist_id           varchar		    distkey,
            year                int             sortkey,
            duration            numeric
        )
""")

artist_table_create = ("""
        CREATE TABLE artists (
            artist_id           varchar         PRIMARY KEY distkey,
            name                varchar,
            location            varchar,
            latitude            numeric,
            longitude           numeric
        )
""")

time_table_create = ("""
        CREATE TABLE time(
            start_time          timestamp       PRIMARY KEY sortkey,
            hour                int,
            day                 int,
            week                int,
            month               int,
            year                int,
            weekday             int
        )
""")

# STAGING TABLES

staging_events_copy = ("""
        COPY staging_events 
            FROM '{}'
            credentials 'aws_iam_role={}'
            region 'us-west-2' 
            JSON '{}'
            timeformat as 'epochmillisecs'
""").format(config.get('S3', 'LOG_DATA'),
			config.get('IAM_ROLE', 'ARN'),
			config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
        COPY staging_songs
            FROM '{}'
            credentials 'aws_iam_role={}'
            region 'us-west-2' 
            JSON 'auto'
""").format(config.get('S3', 'SONG_DATA'),
			config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT
            DISTINCT(e.ts)      AS start_time, 
            e.userId            AS user_id, 
            e.level             AS level,
            s.song_id           AS song_id, 
            s.artist_id         AS artist_id, 
            e.sessionId         AS session_id, 
            e.location          AS location, 
            e.userAgent         AS user_agent
        FROM staging_events e JOIN staging_songs s ON (e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration)
            WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT
            DISTINCT(userId)    AS user_id,
            firstName           AS first_name,
            lastName            AS last_name,
            gender,
            level
        FROM staging_events
            WHERE user_id IS NOT NULL AND page = 'NextSong'
""")

song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT
            DISTINCT(song_id)   AS song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
            WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT  
            DISTINCT(artist_id) AS artist_id,
            artist_name         AS name,
            artist_location     AS location,
            artist_latitude     AS latitude,
            artist_longitude    AS longitude
        FROM staging_songs
            WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT
            DISTINCT(start_time)			        AS start_time,
            EXTRACT(hour        FROM start_time)    AS hour,
            EXTRACT(day         FROM start_time)    AS day,
            EXTRACT(week        FROM start_time)    AS week,
            EXTRACT(month       FROM start_time)    AS month,
            EXTRACT(year        FROM start_time)    AS year,
            EXTRACT(dayofweek   FROM start_time)    AS weekday
        FROM songplays
""")

# Some Analytics Queries
get_count_staging_events = "SELECT COUNT(*) FROM staging_events"
get_count_staging_songs = "SELECT COUNT(*) FROM staging_songs"
get_count_songplays = "SELECT COUNT(*) FROM songplays"
get_count_users = "SELECT COUNT(*) FROM users"
get_count_songs = "SELECT COUNT(*) FROM songs"
get_count_artists = "SELECT COUNT(*) FROM artists"
get_count_time = "SELECT COUNT(*) FROM time"

check_most_played_songs = """
    SELECT u.last_name,
    	u.first_name,
        s.title,
        count(sp.songplay_id) AS playted_times
    FROM songplays AS sp
            JOIN users   AS u ON (u.user_id = sp.user_id)
            JOIN songs   AS s ON (s.song_id = sp.song_id)
            JOIN artists AS a ON (a.artist_id = sp.artist_id)
            JOIN time    AS t ON (t.start_time = sp.start_time)
    GROUP BY u.last_name, u.first_name, s.title
    ORDER BY count(sp.songplay_id) DESC
    LIMIT 100;
"""

check_played_songs_by_user = """
    SELECT  a.name AS Artist_Name,
            s.title AS Song_Title,
        	count(sp.songplay_id) AS playted_times
    FROM songplays AS sp
            JOIN users   AS u ON (u.user_id = sp.user_id)
            JOIN songs   AS s ON (s.song_id = sp.song_id)
            JOIN artists AS a ON (a.artist_id = sp.artist_id)
            JOIN time    AS t ON (t.start_time = sp.start_time)
    WHERE u.first_name = 'Chloe'
    	AND u.last_name = 'Cuevas'
    GROUP BY a.name, s.title
    ORDER BY count(sp.songplay_id) DESC
    LIMIT 100;
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
analytics_queries = [get_count_staging_events,get_count_staging_songs,get_count_songplays,get_count_users,get_count_songs,get_count_artists,get_count_time,check_most_played_songs,check_played_songs_by_user]
