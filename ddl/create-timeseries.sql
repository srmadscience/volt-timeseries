file -inlinebatch END_OF_BATCH

DROP FUNCTION VoltTimeSeriesgetMinDate IF EXISTS;
DROP FUNCTION VoltTimeSeriesgetMaxDate IF EXISTS;
DROP FUNCTION VoltTimeSeriesgetMinValue IF EXISTS;
DROP FUNCTION VoltTimeSeriesgetMaxValue IF EXISTS;
DROP FUNCTION VoltTimeSeriesfindValueForExactMatch             IF EXISTS;
DROP FUNCTION VoltTimeSeriesfindValueForNearestMatch             IF EXISTS;
DROP FUNCTION VoltTimeSeriesgetEntryCount             IF EXISTS;
DROP FUNCTION VoltTimeSeriesput             IF EXISTS;
DROP FUNCTION VoltTimeSeriesputFirst IF EXISTS;
DROP FUNCTION VoltTimeSeriestoString IF EXISTS;

DROP FUNCTION VoltTimeSeriesgetOffsetBytes IF EXISTS;
DROP FUNCTION VoltTimeSeriesgetOffsetDecimals IF EXISTS;
DROP FUNCTION VoltTimeSeriesgetGranularityBytes IF EXISTS;
DROP FUNCTION VoltTimeSeriesgetGranularityDecimals IF EXISTS;
DROP FUNCTION VoltTimeSeriesgetPayloadSize IF EXISTS;

END_OF_BATCH


LOAD CLASSES ../jars/voltdb-timeseries.jar;


file -inlinebatch END_OF_BATCH


CREATE FUNCTION VoltTimeSeriesgetMinDate FROM METHOD ie.voltdb.timeseries.VoltTimeSeries.getMinDate;
CREATE FUNCTION VoltTimeSeriesgetMaxDate FROM METHOD ie.voltdb.timeseries.VoltTimeSeries.getMaxDate;
CREATE FUNCTION VoltTimeSeriesgetMinValue FROM METHOD ie.voltdb.timeseries.VoltTimeSeries.getMinValue;
CREATE FUNCTION VoltTimeSeriesgetMaxValue FROM METHOD ie.voltdb.timeseries.VoltTimeSeries.getMaxValue;
CREATE FUNCTION VoltTimeSeriesfindValueForExactMatch FROM METHOD ie.voltdb.timeseries.VoltTimeSeries.findValueForExactMatch;
CREATE FUNCTION VoltTimeSeriesfindValueForNearestMatch FROM METHOD ie.voltdb.timeseries.VoltTimeSeries.findValueForNearestMatch;
CREATE FUNCTION VoltTimeSeriesgetEntryCount FROM METHOD ie.voltdb.timeseries.VoltTimeSeries.getEntryCount;
CREATE FUNCTION VoltTimeSeriesput FROM METHOD ie.voltdb.timeseries.VoltTimeSeries.put;
CREATE FUNCTION VoltTimeSeriesputFirst FROM METHOD ie.voltdb.timeseries.VoltTimeSeries.putFirst;
CREATE FUNCTION VoltTimeSeriestoString FROM METHOD ie.voltdb.timeseries.VoltTimeSeries.toString;

CREATE FUNCTION VoltTimeSeriesgetOffsetBytes FROM METHOD ie.voltdb.timeseries.VoltTimeSeries.getOffsetBytes;
CREATE FUNCTION VoltTimeSeriesgetOffsetDecimals FROM METHOD ie.voltdb.timeseries.VoltTimeSeries.getOffsetDecimals;
CREATE FUNCTION VoltTimeSeriesgetGranularityBytes FROM METHOD ie.voltdb.timeseries.VoltTimeSeries.getGranularityBytes;
CREATE FUNCTION VoltTimeSeriesgetGranularityDecimals FROM METHOD ie.voltdb.timeseries.VoltTimeSeries.getGranularityDecimals;
CREATE FUNCTION VoltTimeSeriesgetPayloadSize FROM METHOD ie.voltdb.timeseries.VoltTimeSeries.getPayloadSize;


END_OF_BATCH
