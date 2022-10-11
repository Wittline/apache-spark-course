-- create database
CREATE DATABASE IF NOT EXISTS emrdb;

-- create table;

-- "number_record","average_rate_per_night","bedrooms_count","city","date_of_listing","description","latitude","longitude","title","url"

CREATE EXTERNAL TABLE emrdb.airbnblistings
    (
    `number_record` 	STRING,
	`average_rate_per_night`	STRING,
	`bedrooms_count`	STRING,
	`city`	STRING,
	`date_of_listing`	STRING,
	`description`	STRING,
	`latitude`	STRING,
	`longitude`	STRING,
	`title`	STRING,
	`url`	STRING
    )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = "",",
"quoteChar" = "\"", 
"escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION 's3://racv-emr-serverless-bucket/datasets/'
TBLPROPERTIES ('skip.header.line.count'='1')