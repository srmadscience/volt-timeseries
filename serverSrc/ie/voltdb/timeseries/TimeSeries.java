/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
package ie.voltdb.timeseries;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltProcedure.VoltAbortException;

public class TimeSeries {

    public static final int HEADER_BYTES = 13;

    public static final int OFFSET_BYTECOUNT_LOCATION = 0;
    public static final int OFFSET_DECIMALCOUNT_LOCATION = 1;
    public static final int PAYLOAD_SIZE_IN_BYTES_LOCATION = 2;
    public static final int PAYLOAD_DIVISOR_SIZE_LOCATION = 3;
    public static final int DECIMAL_PLACES_LOCATION = 4;
    public static final int MINDATE_8BYTES_LOCATION = 5;

    public static final int TRAILING_DATE_BYTES = 8;

    protected Date minTime = null;
    protected Date maxTime = null;

    protected long minValue = Long.MAX_VALUE;
    protected long maxValue = Long.MIN_VALUE;
    protected boolean minAndMaxValueAreUnreliable = false;

    protected ArrayList<TimeSeriesElement> timeData = null;

    byte decimalPlaces = 0;
    BigDecimal multiplier = new BigDecimal(1);

    // 'public' needed for testing...
    public TimeSeries() {

    }

    // 'public' needed for testing...
    public TimeSeries(byte decimalPlaces) {
        this.decimalPlaces = decimalPlaces;
        multiplier = new BigDecimal(Math.pow(10, decimalPlaces));

    }

    /**
     * Constructor for non-compressed time series
     *
     * @param payload
     */
    public TimeSeries(byte[] payload) {

        byte[] mindateAsByteArray = new byte[Long.BYTES];

        System.arraycopy(payload, MINDATE_8BYTES_LOCATION, mindateAsByteArray, 0, Long.BYTES);
        minTime = new Date(bytesToLong(mindateAsByteArray));
        maxTime = new Date(minTime.getTime());

        int recordLength = payload[OFFSET_BYTECOUNT_LOCATION] + payload[PAYLOAD_SIZE_IN_BYTES_LOCATION];
        int recordCount = (payload.length - HEADER_BYTES) / recordLength;

        for (int i = 0; i < recordCount; i++) {

            byte[] recordDate = new byte[Long.BYTES];
            byte[] recordValue = new byte[Long.BYTES];

            System.arraycopy(payload, HEADER_BYTES + (i * recordLength), recordDate, 0, Long.BYTES);
            System.arraycopy(payload, HEADER_BYTES + Long.BYTES + (i * 16), recordValue, 0, Long.BYTES);

            put(new Date(bytesToLong(recordDate)), bytesToLong(recordValue));
        }

        maxTime = getMaxDateFromByteArray(payload, maxTime);

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

                maxTime = eventTime;

                if (lastValue != e.getValue()) {

                    timeData.add(e);

                    minAndMaxValueAreUnreliable = true;

                } else {
                    return false;
                }

            } else if (eventTime.equals(maxTime)) {

                // update previous value..
                timeData.get(timeData.size() - 1).setValue(value);

            } else if (eventTime.before(minTime)) {

                timeData.add(0, e);
                minTime = eventTime;

            } else {

                int elementId = findExactMatchLocation(eventTime);

                if (elementId > -1) {
                    timeData.get(elementId).setValue(value);
                    minAndMaxValueAreUnreliable = true;

                } else {

                    int nextElementId = findFirstLocationEqualOrAfter(eventTime);
                    timeData.add(nextElementId, e);

                }

            }

            return true;
        }
    }

    /**
     * Add an entry to the TimeSeries. Note that in some cases this is a null-op if
     * there is no change to the value we don't add an entry.
     *
     * @param eventTime
     * @param value
     * @return true if the array of byte has changed
     * @throws BigDecimalHasWrongScaleException
     */
    public boolean put(Date eventTime, BigDecimal value) throws BigDecimalHasWrongScaleException {

        if (decimalPlaces > 0 && value.scale() > decimalPlaces) {
            throw new BigDecimalHasWrongScaleException("Value scale is too big:" + value.toPlainString());
        }

        if (decimalPlaces == 0) {
            return put(eventTime, value.longValue());
        }

        if (decimalPlaces < 0) {
            long foo = value.divide(multiplier, RoundingMode.HALF_EVEN).longValue();

            return put(eventTime, foo);
        }

        return put(eventTime, multiplier.multiply(value).longValue());

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

    public int findExactMatchLocation(Date aTime) {

        for (int i = 0; timeData != null && i < timeData.size(); i++) {

            if (timeData.get(i).getEventTime().equals(aTime)) {
                return i;
            }

            if (timeData.get(i).getEventTime().after(aTime)) {
                return Integer.MIN_VALUE;
            }
        }

        return Integer.MIN_VALUE;
    }

    private int findFirstLocationEqualOrAfter(Date eventTime) {

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

    public BigDecimal findValueForExactMatchBigDecimal(Date aTime) throws BigDecimalHasWrongScaleException {

        int thisElement = findExactMatchLocation(aTime);

        if (thisElement == Integer.MIN_VALUE) {
            return null;
        }

        if (decimalPlaces == 0) {
            return new BigDecimal(timeData.get(thisElement).getValue());
        }

        return convertToBigDecimal(timeData.get(thisElement).getValue());

    }

    public long findValueForExactMatch(Date referenceTime) throws BigDecimalHasWrongScaleException {

        if (decimalPlaces > 0) {
            throw new BigDecimalHasWrongScaleException(
                    "Attempting to use long to get a value stored with " + decimalPlaces + " decimal places");
        }

        int thisElement = findExactMatchLocation(referenceTime);

        if (thisElement == Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }

        if (decimalPlaces == 0) {
            return timeData.get(thisElement).getValue();
        }

        return (long) (Math.pow(timeData.get(thisElement).getValue(), decimalPlaces));
    }

    public BigDecimal findValueForFirstLocationEqualOrAfterBigDecimal(Date aTime)
            throws BigDecimalHasWrongScaleException {

        int thisElement = findFirstLocationEqualOrAfter(aTime);

        if (thisElement == Integer.MIN_VALUE) {
            return null;
        }

        if (decimalPlaces == 0) {
            return new BigDecimal(timeData.get(thisElement).getValue());
        }

        return convertToBigDecimal(timeData.get(thisElement).getValue());

    }

    public long findValueForFirstLocationEqualOrAfter(Date referenceTime) throws BigDecimalHasWrongScaleException {

        if (decimalPlaces > 0) {
            throw new BigDecimalHasWrongScaleException(
                    "Attempting to use long to get a value stored with " + decimalPlaces + " decimal places");
        }

        int thisElement = findFirstLocationEqualOrAfter(referenceTime);

        if (thisElement == Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }

        if (decimalPlaces == 0) {
            return timeData.get(thisElement).getValue();
        }

        return (long) (Math.pow(timeData.get(thisElement).getValue(), decimalPlaces));
    }

    protected BigDecimal convertToBigDecimal(long value) {

        if (decimalPlaces == 0) {
            return new BigDecimal(value);
        }

        BigDecimal bdValue = new BigDecimal(value);

        if (decimalPlaces > 0) {
            return bdValue.setScale(decimalPlaces).divide(multiplier, RoundingMode.HALF_EVEN);
        }

        return bdValue.multiply(multiplier).setScale(0, RoundingMode.HALF_UP);

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

        byte[] metadata = { offsetBytes, offsetDecimals, payloadBytes, payloadDecimals, 0 };

        int spaceNeededForElements = 0;

        if (minTime != null) {
            spaceNeededForElements = 8 + (Long.BYTES + Long.BYTES) * timeData.size(); // TODO
        }

        ByteBuffer buffer = ByteBuffer.allocate(5 + spaceNeededForElements); // TODO

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

    /**
     * @param payload
     * @param defaultDate
     * @return
     */
    protected static Date getMaxDateFromByteArray(byte[] payload, Date defaultDate) {

        if (payload == null || payload.length < 12) {
            return defaultDate;
        }

        byte[] maxdateAsByteArray = new byte[Long.BYTES];
        try {
            System.arraycopy(payload, payload.length - maxdateAsByteArray.length, maxdateAsByteArray, 0, Long.BYTES);
        } catch (Exception e) {
            throw new VoltAbortException("getMaxDateFromByteArray: System.arraycopy failed with "
                    + e.getClass().getName() + ":" + e.getMessage() + ":" + defaultDate.toGMTString() );

        }
        Date maxTime = new Date(bytesToLong(maxdateAsByteArray));
        return maxTime;
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
        builder.append(", decimalPlaces=");
        builder.append(decimalPlaces);
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

    /**
     * @return the decimalPlaces
     */
    public byte getDecimalPlaces() {
        return decimalPlaces;
    }

    /**
     * Expand the column 'columnName' into multiple rows, each with
     * 'columnName'_DATE and 'columnName'_VALUE.
     *
     * @param r
     * @param columnName
     * @return a bigger VoltTable
     */
    @SuppressWarnings("removal")
    public static VoltTable expand(VoltTable r, String columnName) {

        VoltTable expandedTable = null;
        VoltTable.ColumnInfo[] newColumnInfo = null;
        VoltTable.ColumnInfo[] oldColumnInfo = null;
        int timestampColumnIndex = 0;
        int valueColumnIndex = 0;
        r.resetRowPosition();

        while (r.advanceRow()) {

            byte[] timeseriesBytes = r.getVarbinary(columnName);
            CompressedTimeSeries timeseries = new CompressedTimeSeries(timeseriesBytes);

            if (expandedTable == null) {

                // Assumption: The number of decimal places in every row is
                // the same...
                oldColumnInfo = r.getTableSchema();
                timestampColumnIndex = r.getColumnIndex(columnName);
                valueColumnIndex = timestampColumnIndex + 1;

                newColumnInfo = new VoltTable.ColumnInfo[oldColumnInfo.length + 1];

                for (int i = 0; i < timestampColumnIndex; i++) {
                    newColumnInfo[i] = oldColumnInfo[i];
                }

                newColumnInfo[timestampColumnIndex] = new VoltTable.ColumnInfo(columnName.toUpperCase() + "_DATE",
                        VoltType.TIMESTAMP);
                newColumnInfo[valueColumnIndex] = new VoltTable.ColumnInfo(columnName.toUpperCase() + "_VALUE",
                        VoltType.BIGINT);

                if (timeseries.getDecimalPlaces() != 0) {
                    newColumnInfo[valueColumnIndex] = new VoltTable.ColumnInfo(columnName.toUpperCase() + "_VALUE",
                            VoltType.DECIMAL);
                }

                for (int i = valueColumnIndex + 1; i < newColumnInfo.length; i++) {
                    newColumnInfo[i] = oldColumnInfo[i - 1];
                }

                expandedTable = new VoltTable(newColumnInfo);

            }

            for (int x = 0; x < timeseries.size(); x++) {

                final Date thisdate = timeseries.getTimeData().get(x).getEventTime();
                Object[] newRow = new Object[newColumnInfo.length];

                for (int j = 0; j < timestampColumnIndex; j++) {
                    newRow[j] = r.get(j);
                }

                newRow[timestampColumnIndex] = thisdate;

                final long thisValue = timeseries.getTimeData().get(x).getValue();

                if (timeseries.getDecimalPlaces() != 0) {
                    final BigDecimal bdValue = timeseries.convertToBigDecimal(thisValue);
                    newRow[valueColumnIndex] = bdValue;
                } else {
                    newRow[valueColumnIndex] = new Long(thisValue);
                }

                for (int j = timestampColumnIndex + 1; j < oldColumnInfo.length; j++) {
                    newRow[j + 1] = r.get(j);
                }

                expandedTable.addRow(newRow);
            }

        }

        return expandedTable;
    }

}
