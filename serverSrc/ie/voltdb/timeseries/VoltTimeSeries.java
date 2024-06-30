/* This file is part of Volt Active Data.
 * Copyright (C) 2008-2024 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package ie.voltdb.timeseries;

import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.types.TimestampType;

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

            theValue = cts.findValueForExactMatch(theDate.asExactJavaDate());

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

    public int getPayloadSize(byte[] theTimeSeries) {

        if (theTimeSeries == null) {
            throw new VoltAbortException("theTimeSeries can not be null");
        }

        return theTimeSeries.length;
    }

}
