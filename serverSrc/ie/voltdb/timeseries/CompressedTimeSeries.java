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
        final int recordCount = (payload.length - HEADER_BYTES) / recordLength;

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
            spaceNeededForElements = 8 + (offsetBytes + payloadBytes) * timeData.size();
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
     * Examine our data and find the most efficient way to represent the
     * time part
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

        if (maxTimeDiffMs <= Byte.MAX_VALUE && maxTimeDiffMs >= Byte.MIN_VALUE) {
            tsg.setStorageBytes((byte) Byte.BYTES);
        } else if (maxTimeDiffMs <= Short.MAX_VALUE && maxTimeDiffMs >= Short.MIN_VALUE) {
            tsg.setStorageBytes((byte) Short.BYTES);
        } else if (maxTimeDiffMs <= Integer.MAX_VALUE && maxTimeDiffMs >= Integer.MIN_VALUE) {
            tsg.setStorageBytes((byte) Integer.BYTES);
        } else {
            tsg.setStorageBytes((byte) Long.BYTES);
        }

        return tsg;
    }

    /**
     * Examine our data and find the most efficient way to represent data values
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

        if (maxDataValueAfterDivision <= Byte.MAX_VALUE && minDataValueAfterDivision >= Byte.MIN_VALUE) {
            tsg.setStorageBytes((byte) Byte.BYTES);
        } else if (maxDataValueAfterDivision <= Short.MAX_VALUE && minDataValueAfterDivision >= Short.MIN_VALUE) {
            tsg.setStorageBytes((byte) Short.BYTES);
        } else if (maxDataValueAfterDivision <= Integer.MAX_VALUE && minDataValueAfterDivision >= Integer.MIN_VALUE) {
            tsg.setStorageBytes((byte) Integer.BYTES);
        } else {
            tsg.setStorageBytes((byte) Long.BYTES);
        }

        return tsg;
    }

    /**
     * Find most efficient way to store date aValue 
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

}
