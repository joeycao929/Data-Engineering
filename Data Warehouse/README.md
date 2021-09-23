# Data Warehouse with AWS Redshift

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

AWS reources:
* [S3](https://aws.amazon.com/s3/)
* [EC2](https://aws.amazon.com/ec2/)
* [Amazon Redshift](https://aws.amazon.com/redshift/)
    
## Project Dataset
The dataset for this project are stored in S3. The file path for each dataset is:
* **Song data** have infomation about songs and artists. All files are in the same directory.
         '''bash s3://udacity-dend/song_data ''' 
* **Log data** have infomation about evnets of the users. There are two different directories. A descriptor file is needed in this case to extract data from the folders by path. 
      ''' bash s3://udacity-dend/log_data '''
      ''' s3://udacity-dend/log_json_path.json ''' (The descriptor file)
      
## Instruction to run
* 1. First fill in the infomation in the **dwh.cfg** file. You can generate **KEY** and **SECRET** by creating a user on [IAM](https://aws.amazon.com/iam/). 
     **Note**: You DO NOT to fill the **host** in CLUSTER section and **arn** in IAM_ROLE section. Those info will be auto filled during runing the script.
    
'''python
[AWS]
key = 
secret = 

[DWH]
dwh_cluster_type = multi-node
dwh_num_nodes = 4
dwh_node_type = dc2.large
dwh_iam_role_name = 
dwh_cluster_identifier =

[S3]
log_data = 's3://udacity-dend/log_data'
log_jsonpath = 's3://udacity-dend/log_json_path.json'
song_data = 's3://udacity-dend/song_data'

[CLUSTER]
host =
dwh_db = 
dwh_db_user = 
dwh_db_password = 
dwh_port = 5439

[IAM_ROLE]
arn = 
'''

* 2. Run the **create_cluster.py** script to set up the cluster and database for the project.
'''bash
python create_cluster.py
''' 
* 3. Run the **create_tables.py** script to create schema in the database.
'''bash
python create_tables.py
''' 
* 4. Run the **etl.py** script to extract data from S3 and then store the data into the dimensional tables.
'''bash
python etl.py
''' 

## Database Schema Design
### Staging Tables
* staging_events
  Columns   |Data Type  
  -----------------------
  artist     |VARCHAR
  auth       |VARCHAR
  firstname  |VARCHAR
  gender     |VARCHAR
  itemInSesion |INTEGER
  lastname     |VARCHAR
  length       |FLOAT
  level     |VARCHAR
  location  |VARCHAR
  method    |VARCHAR(4)
  page      |VARCHAR
  registration  |VARCHAR
  sessionId     |INTEGER
  song          |VARCHAR
  status        |INTEGER
  ts            |TIMESTAMP
  userAgent     |VARCHAR
  userId        |INTEGER
  
* staging_songs
  Columns   |Data Type  
  -----------------------
  num_songs |INTEGER
  artist_id |VARCHAR
  artist_latitude   |FLOAT
  artist_longitude  |FLOAT
  artist_location   |VARCHAR
  artist_name       |VARCHAR
  song_id           |VARCHAR
  title             |VARCHAR
  duration          |FLOAT
  year              |INTEGER
  
### Fact Table
* songplays
  Columns   |Data Type  |Features |Reference
  -------------------------------
  songplay_id  |INTEGER |IDENTITY(0,1) PRIMARY KEY| 
  start_time   |TIMESTAMP  |NOT NULL |REFERENCES time(start_time) sortkey
  user_id      |INTEGER    |NOT NULL |REFERENCES users(user_id) distkey
  level        |VARCHAR(10) |NOT NULL
  song_id      |VARCHAR(100) |NOT NULL |REFERENCES songs(song_id)
  artist_id    |VARCHAR(50)  |NOT NULL |REFERENCES artists(artist_id)
  sessionId    |INTEGER      |NOT NULL |
  location     |VARCHAR(100) |   |
  user_agent   |VARCHAR |NOT NULL  |
  
### Dimension Table
* users - user_id, first_name, last_name, gender, level
* songs - song_id, title, artist_id, year, duration
* artists - artist_id, name, location, lattitude, longitude
* time - start_time, hour, day, week, month, year, weekday

## Analysis 
Number of rows in each table after loading:
Table Name | Number of Rows
---------------------------
staging_events | 8056
staging_songs |14896
songplays |333
users |104
songs |14896
artists |10025
time |8023

