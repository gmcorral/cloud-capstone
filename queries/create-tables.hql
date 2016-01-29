################
# S3 tables
################

# airline-ontime
# Year - Month - DayofMonth - DayOfWeek - UniqueCarrier - FlightNum - Origin - Dest - CRSDepTime - DepDelay - CRSArrTime - ArrDelay - Cancelled
CREATE EXTERNAL TABLE airline_ontime (year INT, month INT, day INT, weekday INT, carrier STRING, flight_num STRING, origin STRING, dest STRING, deptime STRING, depdelay INT, arrtime STRING, arrdelay INT, cancelled INT)
PARTITIONED BY (date string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
LOCATION 's3n://airline-ontime/';

MSCK REPAIR TABLE airline_ontime;

################
# DynamoDB tables
################

DROP TABLE IF EXISTS group2_ex1;

CREATE EXTERNAL TABLE group2_ex1 (airport STRING, carrier STRING, mean_delay BIGINT)
    STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' 
    TBLPROPERTIES ( 
     "dynamodb.table.name" = "group2_ex1",
	 "dynamodb.region" = "us-east-1",
     "dynamodb.throughput.write.percent" = "1", 
     "dynamodb.column.mapping" = "airport:airport,carrier:carrier,mean_delay:mean_delay");
     
DROP TABLE IF EXISTS group2_ex2;

CREATE EXTERNAL TABLE group2_ex2 (airport STRING, destination STRING, mean_delay BIGINT)
    STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' 
    TBLPROPERTIES ( 
     "dynamodb.table.name" = "group2_ex2",
	 "dynamodb.region" = "us-east-1",
     "dynamodb.throughput.write.percent" = "1", 
     "dynamodb.column.mapping" = "airport:airport,destination:destination,mean_delay:mean_delay");
     
DROP TABLE IF EXISTS group2_ex4;

CREATE EXTERNAL TABLE group2_ex4 (origin STRING, destination STRING, mean_delay BIGINT)
    STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' 
    TBLPROPERTIES ( 
     "dynamodb.table.name" = "group2_ex4",
	 "dynamodb.region" = "us-east-1",
     "dynamodb.throughput.write.percent" = "1", 
     "dynamodb.column.mapping" = "origin:origin,destination:destination,mean_delay:mean_delay");
     
DROP TABLE IF EXISTS group3_ex2;

CREATE EXTERNAL TABLE group3_ex2 (route STRING, depdate STRING, flight_xy STRING, flight_yz STRING)
    STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler' 
    TBLPROPERTIES ( 
     "dynamodb.table.name" = "group3_ex2",
	 "dynamodb.region" = "us-east-1",
     "dynamodb.throughput.write.percent" = "1", 
     "dynamodb.column.mapping" = "route:route,depdate:depdate,flight_xy:flight_xy,flight_yz:flight_yz");