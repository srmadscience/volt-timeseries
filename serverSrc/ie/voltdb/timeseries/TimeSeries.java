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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;

public class TimeSeries {

    public static final int HEADER_BYTES = 12;

    public static final int OFFSET_BYTE = 0;
    public static final int OFFSET_DECIMALS = 1;
    public static final int GRANULARITY_BYTE = 2;
    public static final int GRANULARITY_DECIMALS = 3;
    public static final int REFDATE_4BYTES_STARTS_AT = 4;

    protected Date minTime = null;
    protected Date maxTime = null;

    protected long minValue = Long.MAX_VALUE;
    protected long maxValue = Long.MIN_VALUE;
    protected boolean minAndMaxValueAreUnreliable = false;

    protected ArrayList<TimeSeriesElement> timeData = null;

    public TimeSeries() {

    }

    /**
     * Customer for non-compressed time series
     *
     * @param payload
     */
    public TimeSeries(byte[] payload) {

        byte[] mindateAsByteArray = new byte[Long.BYTES];

        System.arraycopy(payload, REFDATE_4BYTES_STARTS_AT, mindateAsByteArray, 0, Long.BYTES);
        minTime = new Date(bytesToLong(mindateAsByteArray));
        maxTime = new Date(minTime.getTime());

        int recordLength = payload[OFFSET_BYTE] + payload[GRANULARITY_BYTE];
        int recordCount = (payload.length - HEADER_BYTES) / recordLength;

        for (int i = 0; i < recordCount; i++) {

            byte[] recordDate = new byte[Long.BYTES];
            byte[] recordValue = new byte[Long.BYTES];

            System.arraycopy(payload, HEADER_BYTES + (i * recordLength), recordDate, 0, Long.BYTES);
            System.arraycopy(payload, HEADER_BYTES + Long.BYTES + (i * 16), recordValue, 0, Long.BYTES);

            put(new Date(bytesToLong(recordDate)), bytesToLong(recordValue));

        }

    }

    /**
     * @return an array of TimeSeriesElement
     */
    public TimeSeriesElement[] toArray() {
        TimeSeriesElement[] theArray = null;

        if (timeData != null) {
            theArray = new TimeSeriesElement[timeData.size()];
            timeData.toArray(theArray);
        }

        return theArray;
    }

    /**
     * Add an entry to the TimeSeries. Note that in some cases this is a null-op if
     * there is no change to the value we don't add an entry.
     *
     * @param eventTime
     * @param value
     * @return true if the array of byte has changed
     */
    public boolean put(Date eventTime, long value) {

        checkMinValue(value);
        checkMaxValue(value);

        TimeSeriesElement e = new TimeSeriesElement(eventTime, value);

        if (timeData == null) {

            timeData = new ArrayList<>();
            this.minTime = new Date(eventTime.getTime());
            this.maxTime = new Date(eventTime.getTime());
            timeData.add(e);
            return true;

        } else {

            if (eventTime.after(maxTime)) {

                // check previous value..

                long lastValue = timeData.get(timeData.size() - 1).getValue();

                if (lastValue != e.getValue()) {

                    timeData.add(e);
                    maxTime = eventTime;

                    minAndMaxValueAreUnreliable = true;

                } else {
                    return false;
                }

            } else if (eventTime.before(minTime)) {

                timeData.add(0, e);
                minTime = eventTime;

            } else {

                int elementId = findExactMatch(eventTime);

                if (elementId > -1) {
                    timeData.get(elementId).setValue(value);
                    minAndMaxValueAreUnreliable = true;

                } else {

                    int nextElementId = findFirstElementEqualOrAfter(eventTime);
                    timeData.add(nextElementId, e);

                }

            }

            return true;
        }
    }

    /**
     * increment max known value if needed.
     * 
     * @param value
     */
    private void checkMaxValue(long value) {
        if (value > maxValue) {
            maxValue = value;
        }
    }

    /**
     * Increment min known value if needed.
     * 
     * @param value
     */
    private void checkMinValue(long value) {
        if (value < minValue) {
            minValue = value;
        }
    }

    public int findExactMatch(Date aTime) {

        for (int i = 0; timeData != null && i < timeData.size(); i++) {

            if (timeData.get(i).getEventTime().equals(aTime)) {
                return i;
            }
        }

        return -1;
    }

    public long findValueForExactMatch(Date aTime) {

        for (int i = 0; timeData != null && i < timeData.size(); i++) {

            if (timeData.get(i).getEventTime().equals(aTime)) {

                return timeData.get(i).getValue();

            }
        }

        return Long.MIN_VALUE;
    }

    private int findFirstElementEqualOrAfter(Date eventTime) {

        if (timeData == null) {
            return Integer.MIN_VALUE;
        }

        if (eventTime.after(maxTime)) {

            return -1;

        } else if (eventTime.before(minTime)) {

            return 0;
        }

        for (int i = 0; i < timeData.size(); i++) {

            if ((timeData.get(i).getEventTime().getTime() == eventTime.getTime()) || timeData.get(i).after(eventTime)) {
                return i;
            }
        }

        return -1;
    }

    public long findValueForFirstElementEqualOrAfter(Date aTime) {

        long value = Long.MIN_VALUE;

        int recordId = findFirstElementEqualOrAfter(aTime);
        value = timeData.get(recordId).getValue();

        return value;
    }

    public long getValue(Date referenceTime) {
        int thisElement = findFirstElementEqualOrAfter(referenceTime);

        if (thisElement == Integer.MIN_VALUE) {
            return -1;
        }

        return (timeData.get(thisElement).getValue());
    }

    /**
     * Convent to byte[] without compression
     * 
     * @return
     */
    public byte[] toBytes() {

        byte offsetBytes = Long.BYTES;
        byte offsetDecimals = (byte) (CompressedTimeSeries.TIME_GRANULARITY.length - 1);
        byte payloadBytes = Long.BYTES;
        byte payloadDecimals = (byte) (CompressedTimeSeries.DATA_GRANULARITY.length - 1);

        byte[] metadata = { offsetBytes, offsetDecimals, payloadBytes, payloadDecimals };

        int spaceNeededForElements = 0;

        if (minTime != null) {
            spaceNeededForElements = 8 + (Long.BYTES + Long.BYTES) * timeData.size(); // TODO
        }

        ByteBuffer buffer = ByteBuffer.allocate(4 + spaceNeededForElements); // TODO

        buffer.put(metadata);

        int skipCount = 0;

        if (minTime != null) {
            try {
                buffer.put(longToBytes(minTime.getTime()));
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            long lastValue = Long.MIN_VALUE;

            for (int i = 0; timeData != null && i < timeData.size(); i++) {
                if (timeData.get(i).getValue() == lastValue) {
                    skipCount++;
                } else {
                    buffer.put(longToBytes(timeData.get(i).getEventTime().getTime()));
                    buffer.put(longToBytes(timeData.get(i).getValue()));
                }

                lastValue = timeData.get(i).getValue();
            }
        }

        if (skipCount > 0) {

            byte[] untrimmedBuffer = buffer.array();

            byte[] trimmedBuffer = new byte[untrimmedBuffer.length - ((offsetBytes + payloadBytes) * skipCount)];

            System.arraycopy(untrimmedBuffer, 0, trimmedBuffer, 0, trimmedBuffer.length);

            return trimmedBuffer;
        }

        return buffer.array();
    }

    /**
     * @return number of entries.
     */
    public int size() {

        if (timeData == null) {
            return 0;
        }

        return timeData.size();
    }

    /**
     * Certain parts of the 'put' method make minValue and maxValue unreliable. This
     * method resets them.
     */
    private void recalcMinAndMax() {

        minValue = Long.MAX_VALUE;
        maxValue = Long.MIN_VALUE;

        for (int i = 0; timeData != null && i < timeData.size(); i++) {

            long value = timeData.get(i).getValue();

            checkMinValue(value);
            checkMaxValue(value);

        }

        minAndMaxValueAreUnreliable = false;
    }

    public static byte[] longToBytes(long l) {
        byte[] result = new byte[Long.BYTES];
        for (int i = Long.BYTES - 1; i >= 0; i--) {
            result[i] = (byte) (l & 0xFF);
            l >>= Byte.SIZE;
        }
        return result;
    }

    public static long bytesToLong(final byte[] b) {
        long result = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            result <<= Byte.SIZE;
            result |= (b[i] & 0xFF);
        }
        return result;
    }

    public static byte[] integerToBytes(int l) {
        byte[] result = new byte[Integer.BYTES];
        for (int i = Integer.BYTES - 1; i >= 0; i--) {
            result[i] = (byte) (l & 0xFF);
            l >>= Byte.SIZE;
        }
        return result;
    }

    public static long bytesToInteger(final byte[] b) {
        int result = 0;
        for (int i = 0; i < Integer.BYTES; i++) {
            result <<= Byte.SIZE;
            result |= (b[i] & 0xFF);
        }
        return result;
    }

    public static byte[] shortToBytes(short l) {
        byte[] result = new byte[Short.BYTES];
        for (int i = Short.BYTES - 1; i >= 0; i--) {
            result[i] = (byte) (l & 0xFF);
            l >>= Byte.SIZE;
        }
        return result;
    }

    public static long bytesToShort(final byte[] b) {
        int result = 0;
        for (int i = 0; i < Short.BYTES; i++) {
            result <<= Byte.SIZE;
            result |= (b[i] & 0xFF);
        }
        return result;
    }

    @SuppressWarnings("deprecation")
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("TimeSeries [minTime=");
        if (minTime != null) {
            builder.append(minTime.toGMTString());
        }
        builder.append(", maxTime=");
        if (maxTime != null) {
            builder.append(maxTime.toGMTString());
        }
        builder.append(", minValue=");
        builder.append(getMinValue());
        builder.append(", maxValue=");
        builder.append(getMaxValue());
        builder.append(", timeData=");
        builder.append(timeData);
        builder.append("]");
        return builder.toString();
    }

    /**
     * @return the minTime
     */
    public Date getMinTime() {
        return minTime;
    }

    /**
     * @return the maxTime
     */
    public Date getMaxTime() {
        return maxTime;
    }

    /**
     * @return the minValue
     */
    public long getMinValue() {

        if (minAndMaxValueAreUnreliable) {
            recalcMinAndMax();
        }

        return minValue;
    }

    /**
     * @return the maxValue
     */
    public long getMaxValue() {

        if (minAndMaxValueAreUnreliable) {
            recalcMinAndMax();
        }

        return maxValue;
    }

    /**
     * @return the timeData
     */
    public ArrayList<TimeSeriesElement> getTimeData() {
        return timeData;
    }

}
