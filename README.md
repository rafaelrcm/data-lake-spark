# Description: 
In this project I will extract data from s3, process the data with spark and write to parquet files to s3.

# S3:
song_data: contains a folder with json files, will be used to create: songs, artists.
log_data: contains a folder with json files, will be used to create: users, time and songplays.

# Fact Table:
songplays: start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

# Dim Tables:
artists: artist_id, name, location, latitude, longitude
songs: song_id, title, artist_id, year, duration
users: user_id, first_name, last_name, gender, level
time: start_time, hour, day, week, month, year, weekday

# AWS Configuration:
- Set the aws configuration in dwh.cfg

# Run the scripts:
- Run the etl.py