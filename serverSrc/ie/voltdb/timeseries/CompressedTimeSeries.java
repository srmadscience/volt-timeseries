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
import java.util.Date;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;

public class CompressedTimeSeries extends TimeSeries {

    public static final int[] TIME_GRANULARITY = { 60000 // minute
            , 1000 // second
            , 100 // tenth second
            , 10 // Hundredth second
            , 1 // ms
    };

    public static final int[] DATA_GRANULARITY = { 10000, 1000, 100, 50, 10, 5, 4, 3, 2, 1 };

    public CompressedTimeSeries() {
        super();
    }

    /**
     * Create a CompressedTimeSeries from a byte[]
     *
     * @param payload
     */
    public CompressedTimeSeries(byte[] payload) {

        final byte offsetBytes = payload[OFFSET_BYTE];
        final byte offsetDecimals = payload[OFFSET_DECIMALS];
        final byte payloadBytes = payload[GRANULARITY_BYTE];
        final byte payloadDecimals = payload[GRANULARITY_DECIMALS];

        byte[] mindateAsByteArray = new byte[Long.BYTES];

        System.arraycopy(payload, REFDATE_4BYTES_STARTS_AT, mindateAsByteArray, 0, Long.BYTES);
        minTime = new Date(bytesToLong(mindateAsByteArray));
        maxTime = new Date(minTime.getTime());
        final int recordLength = offsetBytes + payloadBytes;
        final int recordCount = (payload.length - HEADER_BYTES - TRAILING_DATE_BYTES) / recordLength;

        long lastTime = minTime.getTime();

        for (int i = 0; i < recordCount; i++) {

            byte[] recordDate = new byte[offsetBytes];
            byte[] recordValue = new byte[payloadBytes];

            System.arraycopy(payload, HEADER_BYTES + (i * recordLength), recordDate, 0, offsetBytes);
            System.arraycopy(payload, HEADER_BYTES + (i * recordLength) + offsetBytes, recordValue, 0, payloadBytes);

            Date recordDateAsDate = null;
            long recordValueAsLong = 0;

            if (offsetBytes == Long.BYTES) {

                recordDateAsDate = new Date(lastTime + (bytesToLong(recordDate) * TIME_GRANULARITY[offsetDecimals]));

            } else if (offsetBytes == Integer.BYTES) {
                recordDateAsDate = new Date(lastTime + (bytesToInteger(recordDate) * TIME_GRANULARITY[offsetDecimals]));

            } else if (offsetBytes == Short.BYTES) {
                recordDateAsDate = new Date(lastTime + (bytesToShort(recordDate) * TIME_GRANULARITY[offsetDecimals]));

            } else if (offsetBytes == Byte.BYTES) {
                recordDateAsDate = new Date(lastTime + (recordDate[0] * TIME_GRANULARITY[offsetDecimals]));

            }

            lastTime = recordDateAsDate.getTime();

            if (payloadBytes == Long.BYTES) {

                recordValueAsLong = bytesToLong(recordValue);

            } else if (payloadBytes == Integer.BYTES) {
                recordValueAsLong = bytesToInteger(recordValue) * DATA_GRANULARITY[payloadDecimals];

            } else if (payloadBytes == Short.BYTES) {
                recordValueAsLong = bytesToShort(recordValue) * DATA_GRANULARITY[payloadDecimals];

            } else if (payloadBytes == Byte.BYTES) {
                recordValueAsLong = recordValue[0] * DATA_GRANULARITY[payloadDecimals];

            }

            put(recordDateAsDate, recordValueAsLong);

        }

        maxTime = getMaxDateFromByteArray(payload,maxTime);

    }

    /**
     * Convert timeseries to byte[]
     */
    @Override
    public byte[] toBytes() {

        TimeSeriesGranularity tsgDate = getDateRepLength();
        TimeSeriesGranularity tsgValue = getpayloadRepLength();

        byte offsetBytes = tsgDate.getStorageBytes();
        byte payloadBytes = tsgValue.getStorageBytes();

        byte[] metadata = { offsetBytes, tsgDate.getDivisorId(), payloadBytes, tsgValue.getDivisorId() };

        int spaceNeededForElements = 0;

        if (minTime != null) {
            spaceNeededForElements = Long.BYTES /* minTime */ + (offsetBytes + payloadBytes) * timeData.size()
                    + Long.BYTES /* maxTime */ ;
        }

        ByteBuffer buffer = ByteBuffer.allocate(4 + spaceNeededForElements);

        buffer.put(metadata);

        int skipCount = 0;

        if (minTime != null) {

            buffer.put(longToBytes(minTime.getTime()));

            long lastTime = minTime.getTime();
            long lastValue = Long.MIN_VALUE;

            for (TimeSeriesElement element : timeData) {

                if (element.getValue() == lastValue) {
                    skipCount++;
                } else {

                    if (tsgDate.getStorageBytes() == Long.BYTES) {

                        buffer.put(longToBytes(element.getEventTime().getTime()));

                    } else if (tsgDate.getStorageBytes() == Integer.BYTES) {

                        long timeDelta = element.getEventTime().getTime() - lastTime;
                        int timeDeltaAfterDecimalPlaces = (int) (timeDelta / TIME_GRANULARITY[tsgDate.getDivisorId()]);
                        buffer.put(integerToBytes(timeDeltaAfterDecimalPlaces));

                    } else if (tsgDate.getStorageBytes() == Short.BYTES) {

                        long timeDelta = element.getEventTime().getTime() - lastTime;
                        short timeDeltaAfterDecimalPlaces = (short) (timeDelta
                                / TIME_GRANULARITY[tsgDate.getDivisorId()]);
                        buffer.put(shortToBytes(timeDeltaAfterDecimalPlaces));

                    } else if (tsgDate.getStorageBytes() == Byte.BYTES) {

                        long timeDelta = element.getEventTime().getTime() - lastTime;
                        byte timeDeltaAfterDecimalPlaces = (byte) (timeDelta
                                / TIME_GRANULARITY[tsgDate.getDivisorId()]);
                        buffer.put(timeDeltaAfterDecimalPlaces);
                    }

                    lastTime = element.getEventTime().getTime();

                    if (tsgValue.getStorageBytes() == Long.BYTES) {
                        buffer.put(longToBytes(element.getValue()));
                    } else if (tsgValue.getStorageBytes() == Integer.BYTES) {

                        long value = element.getValue();
                        int valueAfterDecimalPlaces = (int) (value / DATA_GRANULARITY[tsgValue.getDivisorId()]);
                        buffer.put(integerToBytes(valueAfterDecimalPlaces));

                    } else if (tsgValue.getStorageBytes() == Short.BYTES) {

                        long value = element.getValue();
                        short valueAfterDecimalPlaces = (short) (value / DATA_GRANULARITY[tsgValue.getDivisorId()]);
                        buffer.put(shortToBytes(valueAfterDecimalPlaces));

                    } else if (tsgValue.getStorageBytes() == Byte.BYTES) {

                        long value = element.getValue();
                        byte valueAfterDecimalPlaces = (byte) (value / DATA_GRANULARITY[tsgValue.getDivisorId()]);
                        buffer.put(valueAfterDecimalPlaces);

                    }

                    lastValue = element.getValue();
                }

            }
            // Store max time
            try {
                buffer.put(longToBytes(lastTime));
            } catch (Exception e) {
                e.printStackTrace();
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
     * Fast 'put' method. In 99% of cases the new value will be later than any seen before 
     * and will fit the same encoding. In this case instead of de-serializing payload and
     * creating an ArrayList we add on a couple of bytes to the end of the payload and update
     * the maxdate at the end.
     * 
     * @param payload
     * @param eventTime
     * @param value
     * @return new payload
     */
    public static byte[] put(byte[] payload, Date eventTime, long value) {

        if (payload == null || payload.length <=4 ) {
            CompressedTimeSeries cts = new CompressedTimeSeries();
            cts.put(eventTime, value);
            return cts.toBytes();
        }

        final byte offsetBytes = payload[OFFSET_BYTE];
        final long offsetDecimals = TIME_GRANULARITY[payload[OFFSET_DECIMALS]];
        final byte payloadBytes = payload[GRANULARITY_BYTE];
        final long payloadDecimals = DATA_GRANULARITY[payload[GRANULARITY_DECIMALS]];

        Date maxTime = getMaxDateFromByteArray(payload,eventTime);



        if (eventTime.after(maxTime)) {

            final long timeDelta = (eventTime.getTime() - maxTime.getTime()) / offsetDecimals;
            final long valueToStore = value / payloadDecimals;

            TimeSeriesGranularity tsg = new TimeSeriesGranularity();
            setStorageBytes(tsg, timeDelta, timeDelta);

            // Check  the divisor will work for us. A remainder means it won't..
            if (value % payloadDecimals == 0) {

                if (tsg.getStorageBytes() <= offsetBytes) {

                    setStorageBytes(tsg, value, value);

                    if (tsg.getStorageBytes() <= payloadBytes) {

                        long lastValue = getLastValue(payload, payloadBytes, payloadDecimals);

                        if (lastValue == value) {

                            // Bump maxTime
                            int lastDatePos = payload.length - (TRAILING_DATE_BYTES);
                            System.arraycopy(longToBytes(maxTime.getTime()), 0, payload, lastDatePos,
                                    TRAILING_DATE_BYTES);

                            return payload;

                        } else {

                            // We can tack new payload onto old byte array instead of having to call the
                            // Constructor
                            byte[] newPayload = new byte[payload.length + offsetBytes + payloadBytes];

                            // copy old stuff. minus maxdate
                            System.arraycopy(payload, 0, newPayload, 0, payload.length - Long.BYTES);

                            byte[] timeBytes = new byte[offsetBytes];
                            byte[] dataBytes = new byte[payloadBytes];

                            timeBytes = storeValueInByteArray(offsetBytes, timeDelta, timeBytes); // TODO where else?

                            dataBytes = storeValueInByteArray(payloadBytes, valueToStore, dataBytes);

                            // Copy date differential
                            System.arraycopy(timeBytes, 0, newPayload,
                                    newPayload.length - (TRAILING_DATE_BYTES + timeBytes.length + dataBytes.length),
                                    timeBytes.length);

                            // Copy encoded value
                            System.arraycopy(dataBytes, 0, newPayload,
                                    newPayload.length - (TRAILING_DATE_BYTES + dataBytes.length), dataBytes.length);

                            // Store max time
                            try {

                                System.arraycopy(longToBytes(eventTime.getTime()), 0, newPayload,
                                        newPayload.length - TRAILING_DATE_BYTES, TRAILING_DATE_BYTES);

                            } catch (Exception e) {
                                
                                e.printStackTrace();
                            }

                            return newPayload;
                        }
                    }
                }
            }

        }

        CompressedTimeSeries cts = new CompressedTimeSeries(payload);
        cts.put(eventTime, value);
        return cts.toBytes();
    }

    /**
     * @param payload
     * @param payloadBytes
     * @param payloadDecimals
     * @return
     */
    private static long getLastValue(byte[] payload, final byte payloadBytes, final long payloadDecimals) {
        // See if last value is same, in which case we just overwrite it...
        int lastValuePos = payload.length - (TRAILING_DATE_BYTES + payloadBytes);
        byte[] lastValueBytes = new byte[payloadBytes];
        System.arraycopy(payload, lastValuePos, lastValueBytes, 0, payloadBytes);
        long lastValue = 0;

        if (payloadBytes == Long.BYTES) {

            lastValue = bytesToLong(lastValueBytes);

        } else if (payloadBytes == Integer.BYTES) {
            lastValue = bytesToInteger(lastValueBytes) * payloadDecimals;

        } else if (payloadBytes == Short.BYTES) {
            lastValue = bytesToShort(lastValueBytes) * payloadDecimals;

        } else if (payloadBytes == Byte.BYTES) {
            lastValue = lastValueBytes[0] * payloadDecimals;

        }
        return lastValue;
    }

    /**
     * @param offsetBytes
     * @param timeDelta
     * @param timeBytes
     * @return
     */
    private static byte[] storeValueInByteArray(final byte offsetBytes, final long timeDelta, byte[] timeBytes) {
        if (offsetBytes == Long.BYTES) {
            timeBytes = longToBytes(timeDelta);
        } else if (offsetBytes == Integer.BYTES) {

            timeBytes = integerToBytes((int) timeDelta);

        } else if (offsetBytes == Short.BYTES) {

            timeBytes = shortToBytes((short) timeDelta);

        } else if (offsetBytes == Byte.BYTES) {

            timeBytes[0] = (byte) timeDelta;
        }
        return timeBytes;
    }

    /**
     * Examine our data and find the most efficient way to represent the time part
     *
     * @return a TimeSeriesGranularity
     */
    protected TimeSeriesGranularity getDateRepLength() {

        TimeSeriesGranularity tsg = new TimeSeriesGranularity();
        tsg.setDivisorId((byte) -1);

        if (timeData == null) {
            tsg.setDivisorId((byte) ((byte) TIME_GRANULARITY.length - 1));
            tsg.setStorageBytes((byte) Long.BYTES);
            return tsg;
        }

        for (TimeSeriesElement element : timeData) {

            byte ourDecimal = getTimeGranularity(element.getEventTime().getTime());

            if (tsg.getDivisorId() < ourDecimal) {
                tsg.setDivisorId(ourDecimal);
            }

            if (tsg.getDivisorId() == TIME_GRANULARITY.length - 1) {
                break;
            }

        }

        long maxTimeDiffMs = 0;

        for (int i = 0; i < timeData.size() - 1; i++) {

            long diffBetweenNandNPlus1 = timeData.get(i + 1).getTimeOffsetMS(timeData.get(i).getEventTime());

            if (diffBetweenNandNPlus1 > maxTimeDiffMs) {
                maxTimeDiffMs = diffBetweenNandNPlus1;
            }

        }

        maxTimeDiffMs /= TIME_GRANULARITY[tsg.getDivisorId()];

        setStorageBytes(tsg, maxTimeDiffMs, maxTimeDiffMs);

        return tsg;
    }

    /**
     * Examine our data and find the most efficient way to represent data values
     *
     * @return
     */
    protected TimeSeriesGranularity getpayloadRepLength() {

        TimeSeriesGranularity tsg = new TimeSeriesGranularity();

        if (timeData == null) {
            tsg.setDivisorId((byte) (DATA_GRANULARITY.length - 1));
            tsg.setStorageBytes((byte) Long.BYTES);

            return tsg;
        }

        tsg.setStorageBytes((byte) Long.BYTES);

        tsg.setDivisorId((byte) (-1));

        for (TimeSeriesElement element : timeData) {

            byte ourDataGranularity = getDataGranularity(element.getValue());

            if (tsg.getDivisorId() < ourDataGranularity) {
                tsg.setDivisorId(ourDataGranularity);
            }

            if (tsg.getDivisorId() == DATA_GRANULARITY.length - 1) {
                break;
            }

        }

        long maxDataValueAfterDivision = Long.MIN_VALUE;
        long minDataValueAfterDivision = Long.MAX_VALUE;

        for (TimeSeriesElement element : timeData) {

            long ourValue = element.getValue();
            ourValue /= DATA_GRANULARITY[tsg.getDivisorId()];

            if (ourValue > maxDataValueAfterDivision) {
                maxDataValueAfterDivision = ourValue;
            }

            if (ourValue < minDataValueAfterDivision) {
                minDataValueAfterDivision = ourValue;
            }

        }

        setStorageBytes(tsg, maxDataValueAfterDivision, minDataValueAfterDivision);

        return tsg;
    }

    /**
     * @param tsg
     * @param maxDataValueAfterDivision
     * @param minDataValueAfterDivision
     */
    private static void setStorageBytes(TimeSeriesGranularity tsg, long maxDataValueAfterDivision,
            long minDataValueAfterDivision) {
        if (maxDataValueAfterDivision <= Byte.MAX_VALUE && minDataValueAfterDivision >= Byte.MIN_VALUE) {
            tsg.setStorageBytes((byte) Byte.BYTES);
        } else if (maxDataValueAfterDivision <= Short.MAX_VALUE && minDataValueAfterDivision >= Short.MIN_VALUE) {
            tsg.setStorageBytes((byte) Short.BYTES);
        } else if (maxDataValueAfterDivision <= Integer.MAX_VALUE && minDataValueAfterDivision >= Integer.MIN_VALUE) {
            tsg.setStorageBytes((byte) Integer.BYTES);
        } else {
            tsg.setStorageBytes((byte) Long.BYTES);
        }
    }

    /**
     * Find most efficient way to store date aValue
     *
     * @param aDateAsLong
     * @return optimal storage for aValue
     */
    public static byte getTimeGranularity(long aDateAsLong) {

        for (int i = 0; i < TIME_GRANULARITY.length; i++) {

            if (aDateAsLong % TIME_GRANULARITY[i] == 0) {
                return (byte) i;
            }
        }

        return (byte) TIME_GRANULARITY[TIME_GRANULARITY.length - 1];

    }

    /**
     * Find most efficient way to store aValue
     *
     * @param aValue
     * @return optimal storage for aValue
     */
    public static byte getDataGranularity(long aValue) {

        for (int i = 0; i < DATA_GRANULARITY.length; i++) {

            if (aValue % DATA_GRANULARITY[i] == 0) {
                return (byte) i;
            }
        }

        return (byte) (DATA_GRANULARITY.length - 1);

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

        VoltTable.ColumnInfo[] oldColumnInfo = r.getTableSchema();
        final int timestampColumnIndex = r.getColumnIndex(columnName);
        final int valueColumnIndex = timestampColumnIndex + 1;

        VoltTable.ColumnInfo[] newColumnInfo = new VoltTable.ColumnInfo[oldColumnInfo.length + 1];

        for (int i = 0; i < timestampColumnIndex; i++) {
            newColumnInfo[i] = oldColumnInfo[i];
        }

        newColumnInfo[timestampColumnIndex] = new VoltTable.ColumnInfo(columnName.toUpperCase() + "_DATE",
                VoltType.TIMESTAMP);
        newColumnInfo[valueColumnIndex] = new VoltTable.ColumnInfo(columnName.toUpperCase() + "_VALUE",
                VoltType.BIGINT);

        for (int i = valueColumnIndex + 1; i < newColumnInfo.length; i++) {
            newColumnInfo[i] = oldColumnInfo[i - 1];
        }

        VoltTable expandedTable = new VoltTable(newColumnInfo);
        r.resetRowPosition();

        while (r.advanceRow()) {

            byte[] timeseriesBytes = r.getVarbinary(columnName);
            CompressedTimeSeries timeseries = new CompressedTimeSeries(timeseriesBytes);

            for (int x = 0; x < timeseries.size(); x++) {

                final Date thisdate = timeseries.getTimeData().get(x).getEventTime();
                final long thisValue = timeseries.getTimeData().get(x).getValue();
                Object[] newRow = new Object[newColumnInfo.length];

                for (int j = 0; j < timestampColumnIndex; j++) {
                    newRow[j] = r.get(j);
                }

                newRow[timestampColumnIndex] = thisdate;
                newRow[valueColumnIndex] = new Long(thisValue);

                for (int j = timestampColumnIndex + 1; j < oldColumnInfo.length; j++) {
                    newRow[j + 1] = r.get(j);
                }

                expandedTable.addRow(newRow);
            }

        }

        return expandedTable;
    }

    public static byte getOffsetBytes(byte[] payload) {
        return payload[OFFSET_BYTE];
    }

    public static int getOffsetDecimals(byte[] payload) {
        return TIME_GRANULARITY[payload[OFFSET_DECIMALS]];
    }

    public static byte getGranularityBytes(byte[] payload) {
        return payload[GRANULARITY_BYTE];
    }

    public static int getGranularityDecimals(byte[] payload) {
        return DATA_GRANULARITY[payload[GRANULARITY_DECIMALS]];
    }

    public int getPayloadSize(byte[] payload) {
        return payload.length;
    }

}
