/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
package ie.voltdb.timeseries;

import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.types.TimestampType;

/**
 * A wrapper class for CompressedTimeSeries that throws VoltAbortExceptions with useful messages when needed.
 */
public class VoltTimeSeries {

    public TimestampType getMinDate(byte[] theTimeSeries) throws VoltAbortException {

        TimestampType theValue = new TimestampType(0);

        if (theTimeSeries == null) {
            throw new VoltAbortException("theTimeSeries can not be null");
        }

        try {

            CompressedTimeSeries cts = new CompressedTimeSeries(theTimeSeries);

            theValue = new TimestampType(cts.getMaxTime());

        } catch (Exception e) {
            throw new VoltAbortException("Unable to deserialize theTimeSeries: " + e.getMessage());
        }

        return theValue;
    }

    public TimestampType getMaxDate(byte[] theTimeSeries) throws VoltAbortException {

        TimestampType theValue = new TimestampType(0);

        if (theTimeSeries == null) {
            throw new VoltAbortException("theTimeSeries can not be null");
        }

        try {

            CompressedTimeSeries cts = new CompressedTimeSeries(theTimeSeries);

            theValue = new TimestampType(cts.getMaxTime());

        } catch (Exception e) {
            throw new VoltAbortException("Unable to deserialize theTimeSeries: " + e.getMessage());
        }

        return theValue;
    }

    public long getMinValue(byte[] theTimeSeries) throws VoltAbortException {

        long theValue = Long.MIN_VALUE;

        if (theTimeSeries == null) {
            throw new VoltAbortException("theTimeSeries can not be null");
        }

        try {

            CompressedTimeSeries cts = new CompressedTimeSeries(theTimeSeries);

            theValue = cts.getMinValue();

        } catch (Exception e) {
            throw new VoltAbortException("Unable to deserialize theTimeSeries: " + e.getMessage());
        }

        return theValue;
    }

    public long getMaxValue(byte[] theTimeSeries) throws VoltAbortException {

        long theValue = Long.MIN_VALUE;

        if (theTimeSeries == null) {
            throw new VoltAbortException("theTimeSeries can not be null");
        }

        try {

            CompressedTimeSeries cts = new CompressedTimeSeries(theTimeSeries);

            theValue = cts.getMaxValue();

        } catch (Exception e) {
            throw new VoltAbortException("Unable to deserialize theTimeSeries: " + e.getMessage());
        }

        return theValue;
    }

    public long findValueForExactMatch(byte[] theTimeSeries, TimestampType theDate) throws VoltAbortException {

        long theValue = Long.MIN_VALUE;

        if (theTimeSeries == null) {
            throw new VoltAbortException("theTimeSeries can not be null");
        }

        if (theDate == null) {
            throw new VoltAbortException("theDate can not be null");
        }

        try {

            CompressedTimeSeries cts = new CompressedTimeSeries(theTimeSeries);

            theValue = cts.findValueForExactMatch(theDate.asExactJavaDate());

        } catch (Exception e) {
            throw new VoltAbortException("Unable to deserialize theTimeSeries: " + e.getMessage());
        }

        return theValue;
    }

    public long findValueForNearestMatch(byte[] theTimeSeries, TimestampType theDate) throws VoltAbortException {

        long theValue = Long.MIN_VALUE;

        if (theTimeSeries == null) {
            throw new VoltAbortException("theTimeSeries can not be null");
        }

        if (theDate == null) {
            throw new VoltAbortException("theDate can not be null");
        }

        try {

            CompressedTimeSeries cts = new CompressedTimeSeries(theTimeSeries);

            theValue = cts.findValueForFirstLocationEqualOrAfter(theDate.asExactJavaDate());

        } catch (Exception e) {
            throw new VoltAbortException("Unable to deserialize theTimeSeries: " + e.getMessage());
        }

        return theValue;
    }

    public long getEntryCount(byte[] theTimeSeries) throws VoltAbortException {

        long theValue = Long.MIN_VALUE;

        if (theTimeSeries == null) {
            throw new VoltAbortException("theTimeSeries can not be null");
        }

        try {

            CompressedTimeSeries cts = new CompressedTimeSeries(theTimeSeries);

            theValue = cts.size();

        } catch (Exception e) {
            throw new VoltAbortException("Unable to deserialize theTimeSeries: " + e.getMessage());
        }

        return theValue;
    }

    public byte[] putOLD(byte[] theTimeSeries, TimestampType theDate, long theValue) throws VoltAbortException {

        byte[] theBytes = null;

        if (theTimeSeries == null) {
            throw new VoltAbortException("theTimeSeries can not be null");
        }

        if (theDate == null) {
            throw new VoltAbortException("theDate can not be null");
        }

        if (theValue == Long.MIN_VALUE) {
            throw new VoltAbortException("theValue can not be null");
        }

        try {

            CompressedTimeSeries cts = new CompressedTimeSeries(theTimeSeries);

            if (cts.put(theDate.asExactJavaDate(), theValue)) {
                theBytes = cts.toBytes();
            } else {
                // Nothing has changed...
                return theTimeSeries;
            }
        } catch (Exception e) {
            throw new VoltAbortException("Unable to deserialize theTimeSeries: " + e.getMessage());
        }

        return theBytes;
    }

    public byte[] put(byte[] theTimeSeries, TimestampType theDate, long theValue) throws VoltAbortException {

        byte[] theBytes = null;

        if (theTimeSeries == null) {
            throw new VoltAbortException("theTimeSeries can not be null");
        }

        if (theDate == null) {
            throw new VoltAbortException("theDate can not be null");
        }

        if (theValue == Long.MIN_VALUE) {
            throw new VoltAbortException("theValue can not be null");
        }

        try {

            theBytes = CompressedTimeSeries.put(theTimeSeries, theDate.asExactJavaDate(), theValue);
        } catch (Exception e) {
            throw new VoltAbortException("Unable to deserialize theTimeSeries: " + e.getMessage());
        }

        return theBytes;
    }

    public byte[] putFirst(TimestampType theDate, long theValue) throws VoltAbortException {

        byte[] theBytes = null;

        if (theDate == null) {
            throw new VoltAbortException("theDate can not be null");
        }

        if (theValue == Long.MIN_VALUE) {
            throw new VoltAbortException("theValue can not be null");
        }

        try {

            CompressedTimeSeries cts = new CompressedTimeSeries();

            cts.put(theDate.asExactJavaDate(), theValue);
            theBytes = cts.toBytes();

        } catch (Exception e) {
            throw new VoltAbortException("Unable to deserialize theTimeSeries: " + e.getMessage());
        }

        return theBytes;
    }

    public String toString(byte[] theTimeSeries) throws VoltAbortException {

        String theString = null;

        if (theTimeSeries == null) {
            throw new VoltAbortException("theDate can not be null");
        }

        try {

            CompressedTimeSeries cts = new CompressedTimeSeries(theTimeSeries);
            theString = cts.toString();

        } catch (Exception e) {
            throw new VoltAbortException("Unable to deserialize theTimeSeries: " + e.getMessage());
        }

        return theString;
    }

    public int getOffsetBytes(byte[] theTimeSeries) {

        if (theTimeSeries == null) {
            throw new VoltAbortException("theTimeSeries can not be null");
        }

        return CompressedTimeSeries.getOffsetBytes(theTimeSeries);
    }

    public int getOffsetDecimals(byte[] theTimeSeries) {

        if (theTimeSeries == null) {
            throw new VoltAbortException("theTimeSeries can not be null");
        }

        return CompressedTimeSeries.getOffsetDecimals(theTimeSeries);
    }

    public int getGranularityBytes(byte[] theTimeSeries) {

        if (theTimeSeries == null) {
            throw new VoltAbortException("theTimeSeries can not be null");
        }

        return CompressedTimeSeries.getGranularityBytes(theTimeSeries);
    }

    public int getGranularityDecimals(byte[] theTimeSeries) {

        if (theTimeSeries == null) {
            throw new VoltAbortException("theTimeSeries can not be null");
        }

        return CompressedTimeSeries.getGranularityDecimals(theTimeSeries);
    }

    public int getGranularityDivisor(byte[] theTimeSeries) {

        if (theTimeSeries == null) {
            throw new VoltAbortException("theTimeSeries can not be null");
        }

        return CompressedTimeSeries.getGranularityDivisor(theTimeSeries);
    }

    public int getPayloadSize(byte[] theTimeSeries) {

        if (theTimeSeries == null) {
            throw new VoltAbortException("theTimeSeries can not be null");
        }

        return theTimeSeries.length;
    }

}
