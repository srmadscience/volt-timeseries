file -inlinebatch END_OF_BATCH

DROP PROCEDURE ReportEvent IF EXISTS;
DROP PROCEDURE GetEvents IF EXISTS;
DROP TABLE normal_timeseries_table IF EXISTS;
DROP TABLE compressed_timeseries_table IF EXISTS;

END_OF_BATCH

LOAD CLASSES ../jars/voltdb-timeseries-demo.jar;

file -inlinebatch END_OF_BATCH


CREATE TABLE normal_timeseries_table 
(message_type_id varchar(30) not null
,message_time     timestamp  not null
,event_value      bigint     not null
,primary key (message_type_id,message_time));

PARTITION TABLE normal_timeseries_table  ON COLUMN message_type_id;

CREATE TABLE compressed_timeseries_table 
(message_type_id varchar(30) not null
,message_time     timestamp  not null
,event_ts         varbinary(1048576) not null
,primary key (message_type_id,message_time));

PARTITION TABLE compressed_timeseries_table  ON COLUMN message_type_id ;


CREATE PROCEDURE  
   PARTITION ON TABLE compressed_timeseries_table COLUMN message_type_id
   FROM CLASS timeseries.ReportEvent;  

CREATE PROCEDURE  
   PARTITION ON TABLE compressed_timeseries_table COLUMN message_type_id
   FROM CLASS timeseries.GetEvents;  

END_OF_BATCH




