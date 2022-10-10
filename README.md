# Sparkify Database Warehouse

## Overview

This project provides the schema and ETL to create and populate a data warehouse
in the cloud for analytics purposes at the music streaming app Sparkify.

The ETL and data warehouse has been built on AWS, with a PostgreSQL database and
staging tables hosted on Amazon Redshift, pulling data from Amazon S3. The
analytics tables have been arranged in a star schema to allow the Sparkify team
to readily run queries to analyze user activity on their app, such as on what
songs users are listening to. The scripts have been created in Python.

## Structure

The project contains the following components:

* `create_tables.py` creates the Sparkify star schema in Redshift
* `etl.py` defines the ETL pipeline, extracting data from S3, loading into staging tables on Redshift, and then processing into analytics tables on Redshift
* `sql_queries.py` defines the SQL queries that underpin the creation of the star schema and ETL pipeline
* `exploratory_analytics.ipynb` allows you to more interactively execute the ETL and run queries

## Database Schema

The database contains the following fact table:

* `songplays` - user song plays

`songplays` has foreign keys to the following (self-explanatory) dimension
tables:

* `users`
* `songs`
* `artists`
* `time`

## Instructions

You will need to create a configuration file with the file name `dwh.cfg` and
the following structure:

```
[CLUSTER]
HOST=<your_host>
DB_NAME=<your_db_name>
DB_USER=<your_db_user>
DB_PASSWORD=<your_db_password>
DB_PORT=<your_db_port>
DB_REGION=<your_db_region>
CLUSTER_IDENTIFIER=<your_cluster_identifier>

[IAM_ROLE]
ARN=<your_iam_role_arn>

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[AWS]
ACCESS_KEY=<your_access_key>
SECRET_KEY=<your_secret_key>
```

To execute the ETL on an existing cluster from the command line, enter the
following:

```
python3 create_tables.py
python3 etl.py
```

Make sure your working directory is at the top-level of the project.

## Query Example

Once you've created the database and run the ETL pipeline, you can test out some
queries in Redshift console query editor:

```
--Check most played songs and with whom
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
```

```
--Played songs by specific user
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
```
