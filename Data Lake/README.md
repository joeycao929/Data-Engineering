# Data Lake
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, we will build an ELT pipeline that loads their data from S3 and processes with Spark and then loads the data back to the S3 as a set of dimensional tables. 

## Project Dataset
The dataset for this project are stored in S3. The file path for each dataset is:
* **Song data** have infomation about songs and artists. All files are in the same directory.
         '''bash s3://udacity-dend/song_data ''' 
* **Log data** have infomation about evnets of the users. 
         ''' bash s3://udacity-dend/log_data '''
      
      
## ELT pipeline
* 1. Load the credentials from dl.cfg
* 2. Load the json files from S3
* 3. Process the data with Spark to generate Fact and Dimension tables.
* 4. Load the tables back to S3

## Dimension Table
* **users**
|Column Name|Data Type|
|:---------:|:-------:|
|user_id| String|
|first_name| String|
|last_name|String|
|gender|String|
|level|String|

* **songs**
|Column Name|Data Type|
|:---------:|:-------:|
|song_id|String|
|title|String|
|artist_id|String|
|year|Interger|
|duration|Double|

* **artists**
|Column Name|Data Type|
|:---------:|:-------:|
|artist_id|String|
|name|String|
|location|String|
|lattitude|Double|
|longitude|Double|

* **time**
|Column Name|Data Type|
|:---------:|:-------:|
|start_time|String|
|hour|Integer|
|day|Integer|
|week|Integer|
|month|Integer|
|year|Integer|
|weekday|Integer|

## Fact Table
* **songplays**
|Column Name|Data Type|
|:---------:|:-------:|
|songplay_id|Integer|
|start_time|String|
|user_id|String|
|level|String|
|song_id|String|
|artist_id|String|
|session_id|String|
|location|String|
|user_agent|String|

## How to run script
* 1. Fill in your aws secret key and id in the **dl.cfg** file
* 2. Run **etl.py** 
    """bash python etl.py"""
     


