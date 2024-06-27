# volt-timeseries
Prototype time series data handling

## Problem: How do we efficiently store potentially millions of small readings?

* IIoT systems generate millions of telemtry data points
* It's not ecomic to store these in a fully normalized data structure, due to row storage overheads

## Solution: A Time Series library

Our data stream looks like this:

|Timestamp | value|
|----------|------|
|12:31:01 45| 10 |
|12:31:02 00| 10 |
|12:31:02 15| 10 |
|12:31:02 45| 12 |
|12:31:03 00| 13 |
|12:31:03 15| 15 |
|---|---|

There are several properties which lend themselves to compression:

* The date sequence is ordered, and increments by 15 seconds each time

* The value sometimes doesn't change

* The value sometimes has a very small range of possible values

## Algorithm

### Compressing the date

By default we'd use a Java Date and a Java Long to store readings. Both are 8 bytes long.

We start by converting all the date fields into Long and trying to find a common divisor. So if, for example, we are getting one reading per second the divisor would be 1000, as Java's time uses millisecond as a base unit. 

We have a list of possible divisors in an array:

````
   public static final int[] TIME_GRANULARITY = { 60000 // minute
            , 1000 // second
            , 100 // tenth second
            , 10 // Hundredth second
            , 1 // ms
    };
````

In the case above the divisor comes out at 1000. '1000' is element '1', so for encoding we'll use '1'.

We then take each date, take the increase in value from the previous one, and divide it by out divisor. In the example about it's always 15, as each record is 15,000 ms apart. We find the maximum increment value, which will still be 15 in this case.

### Compressing the value

We have a list of plausible divisors for our value:

````
public static final int[] DATA_GRANULARITY = { 10000, 1000, 100, 50, 10, 5, 4, 3, 2, 1 };
````
In the example above we'll end up with a divisor of '1', which is element 9. 

We scan all the values and see if they are Bytes, Shorts, Ints or Longs. We note the worst case scenario.

### Encoding

We can then encode the data like this:


|Byte | value|Meaning|
|----------|------|---|
|0| 1| multiply date delta field by TIME_GRANULARITY[1] or 1000|
|1| 2 | use a java byte to store time delta 
|2| 9 | multiply value field by DATA_GRANULARITY[9] or 1|
|3| 2 |use a java byte to store value  |
|4-11| 12:31:01 45|start  date|


Then (in this case) pairs  of bytes with the actual data:

|Byte 1|Byte 2|
|---|---|
|15|10|
|60|12|
|15|13|
|15|15


Note that we skip the entries for '12:31:02 00' and '12:31:03 00', as they don't show a change in value.

## Decoding

We reverse the process above, but note that when reloading we may lose pointless data:

This means that when we deserialize our byte array we get:


|Timestamp | value|
|----------|------|
|12:31:01 45| 10 |
|12:31:02 45| 12 |
|12:31:03 00| 13 |
|12:31:03 15| 15 |
|---|---|

## Expanding decoded data 

### Within SQL

The VoltTimeSeriestoString function turns the varbinary format into a human readable one:

````3> select * from compressed_timeseries_table;

MESSAGE_TYPE_ID  MESSAGE_HOUR                EVENT_TS                                                                                   
---------------- --------------------------- -------------------------------------------------------------------------------------------
FOO4             2024-06-27 16:00:00.000000  02040109000001905A8C6B7E0000000F9F010FA0000FA0012710000FA00107D0001F40010FA00207D00007D... 
FOO4             2024-06-27 17:00:00.000000  02040109000001905AA3C280000001018D0207D00107D00007D00207D00007D00207D00007D00207D00007D... 

(Returned 2 rows in 0.41s)
4> select MESSAGE_TYPE_ID, MESSAGE_HOUR, VoltTimeSeriestoString(event_ts) from compressed_timeseries_table;
MESSAGE_TYPE_ID  MESSAGE_HOUR                C3                                                                                         
---------------- --------------------------- -------------------------------------------------------------------------------------------
FOO4             2024-06-27 16:00:00.000000  TimeSeries [minTime=27 Jun 2024 16:34:30 GMT, maxTime=27 Jun 2024 16:59:58 GMT, minValue=0, maxValue=2, timeData=[TimeSeriesElement [eventTime=27 Jun 2024 16:34:30 GMT, value=0], TimeSeriesElement [eventTime=27 Jun 2024 16:34:34 GMT, value=1], TimeSeriesElement [eventTime=27 Jun 2024 16:34:38 GMT, value=0], TimeSeriesElement [eventTime=27 Jun 2024 16:34:42 GMT, value=1], TimeSeriesElement [eventTime=27 Jun 2024 16:34:52 GMT, value=0], TimeSeriesElement [eventTime=27 Jun 2024 16:34:56 GMT, value=1], TimeSeriesElement [eventTime=27 Jun 2024 16:34:58 GMT, value=0], TimeSeriesElement [eventTime=27 Jun 2024 16:35:06 GMT, value=1], TimeSeriesElement [eventTime=27 Jun 2024 16:35:10 GMT, value=2], TimeSeriesElement [eventTime=27 Jun 2024 16:35:12 GMT, value=0], TimeSeriesElement [eventTime=27 Jun 2024 16:35:14 GMT, value=1], TimeSeriesElement [eventTime=27 Jun 2024 16:35:18 GMT, value=0], TimeSeriesElement [eventTime=27 Jun 2024 16:35:22 GMT, value=1], TimeSeriesElement [eventTime=27 Jun 2024 16:35:24 GMT, value=0], TimeSeriesElement [eventTime=27 Jun 2024 16:35:26 GMT, value=2], TimeSeriesElement [eventTime=27 Jun 2024 16:35:28 GMT, value=0], TimeSeriesElement [eventTime=27 Jun 2024 16:35:30 GMT, value=1], TimeSeriesElement [eventTime=27 Jun 
.... (truncated for brevity)


(Returned 2 rows in 2.68s)
5> 
````



### Within Java

The library function CompressedTimeSeries.expand goes through a VoltTable and expands individual rows containing time series data into a 'normal' format. the the case below a row containing a varbinary field called 'EVENT_TS' is expanded into many rows containing EVENT_TS_DATE and EVENT_TS_VALUE

````
public class GetEvents extends VoltProcedure {

	// @formatter:off

        
    public static final SQLStmt getCompressed = new SQLStmt(
            "SELECT * from compressed_timeseries_table WHERE message_type_id = ? AND message_hour BETWEEN TRUNCATE(HOUR,?) AND TRUNCATE(HOUR,?) order by message_hour;");
            
    // @formatter:on

	public VoltTable[] run(String messageTypeId, TimestampType startTime, TimestampType endTime) throws VoltAbortException {

		voltQueueSQL(getCompressed,messageTypeId,  startTime,  endTime);
		
		VoltTable[] results = voltExecuteSQL(true);
		
		results[0] = CompressedTimeSeries.expand(results[0], "EVENT_TS");

	
		return results;

	}

}

````

Will take a single row and return something like:




