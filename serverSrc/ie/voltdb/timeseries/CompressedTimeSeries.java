/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package ie.voltdb.timeseries;

import java.nio.ByteBuffer;
import java.util.Date;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;

public class CompressedTimeSeries extends TimeSeries {

    public static final int[] TIME_GRANULARITY = { 3600000 // 1 hour
            , 1800000 // 30 minute
            , 600000 // 10 minute
            , 60000 // minute
            , 1000 // second
            , 100 // tenth second
            , 10 // Hundredth second
            , 1 // ms
    };

    public static final int[] DATA_GRANULARITY = { 1000000000, 100000000, 10000000, 1000000, 100000, 10000, 1000, 100,
            50, 10, 5, 4, 3, 2, 1 };

    public CompressedTimeSeries() {
        super();
    }

    public CompressedTimeSeries(byte decimalPlaces) {
        super(decimalPlaces);
    }

    /**
     * Create a CompressedTimeSeries from a byte[]
     *
     * @param payload
     */
    public CompressedTimeSeries(byte[] payload) {

        final byte offsetBytes = payload[OFFSET_BYTECOUNT_LOCATION];
        final byte offsetDecimals = payload[OFFSET_DECIMALCOUNT_LOCATION];
        final byte payloadBytes = payload[PAYLOAD_SIZE_IN_BYTES_LOCATION];
        final byte payloadDivisor = payload[PAYLOAD_DIVISOR_SIZE_LOCATION];
        decimalPlaces = payload[DECIMAL_PLACES_LOCATION];

        byte[] mindateAsByteArray = new byte[Long.BYTES];

        System.arraycopy(payload, MINDATE_8BYTES_LOCATION, mindateAsByteArray, 0, Long.BYTES);
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
                recordValueAsLong = bytesToInteger(recordValue) * DATA_GRANULARITY[payloadDivisor];

            } else if (payloadBytes == Short.BYTES) {
                recordValueAsLong = bytesToShort(recordValue) * DATA_GRANULARITY[payloadDivisor];

            } else if (payloadBytes == Byte.BYTES) {
                recordValueAsLong = recordValue[0] * DATA_GRANULARITY[payloadDivisor];

            }

            put(recordDateAsDate, recordValueAsLong);

        }

        maxTime = getMaxDateFromByteArray(payload, maxTime);

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

        byte[] metadata = { offsetBytes, tsgDate.getDivisorId(), payloadBytes, tsgValue.getDivisorId(), decimalPlaces };

        int spaceNeededForElements = 0;

        if (minTime != null) {
            spaceNeededForElements = Long.BYTES /* minTime */ + (offsetBytes + payloadBytes) * timeData.size()
                    + Long.BYTES /* maxTime */ ;
        }

        ByteBuffer buffer = ByteBuffer.allocate(5 + spaceNeededForElements);

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
     * Fast 'put' method. In 99% of cases the new value will be later than any seen
     * before and will fit the same encoding. In this case instead of de-serializing
     * payload and creating an ArrayList we add on a couple of bytes to the end of
     * the payload and update the maxdate at the end.
     *
     * @param payload
     * @param eventTime
     * @param value
     * @return new payload
     */
    public static byte[] put(byte[] payload, Date eventTime, long value) {

        if (payload == null || payload.length <= 5) {
            CompressedTimeSeries cts = new CompressedTimeSeries();
            cts.put(eventTime, value);
            return cts.toBytes();
        }

        final byte offsetBytes = payload[OFFSET_BYTECOUNT_LOCATION];
        final long offsetDecimals = TIME_GRANULARITY[payload[OFFSET_DECIMALCOUNT_LOCATION]];
        final byte payloadBytes = payload[PAYLOAD_SIZE_IN_BYTES_LOCATION];
        final long payloadDivisor = DATA_GRANULARITY[payload[PAYLOAD_DIVISOR_SIZE_LOCATION]];

        Date maxTime = getMaxDateFromByteArray(payload, eventTime);

        if (eventTime.after(maxTime)) {

            final long timeDelta = (eventTime.getTime() - maxTime.getTime()) / offsetDecimals;
            final long valueToStore = value / payloadDivisor;

            TimeSeriesGranularity tsg = new TimeSeriesGranularity();
            tsg.setStorageBytes(timeDelta, timeDelta);

            // Check the divisor will work for us. A remainder means it won't..
            if (value % payloadDivisor == 0) {

                if (tsg.getStorageBytes() <= offsetBytes) {

                    tsg.setStorageBytes(value, value);

                    if (tsg.getStorageBytes() <= payloadBytes) {

                        long lastValue = getLastValue(payload, payloadBytes, payloadDivisor);

                        if (lastValue == value) {

                            // Bump maxTime
                            int lastDatePos = payload.length - (TRAILING_DATE_BYTES);
                            System.arraycopy(longToBytes(maxTime.getTime()), 0, payload, lastDatePos,
                                    TRAILING_DATE_BYTES);
                            // Fast behavior #1 - change maxtime at end of serialized array...
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
                            // Fast behavior #2 - add entry to end of serialized array...
                            return newPayload;
                        }
                    }
                }
            }

        }

        // Default behavior - create java object, add entry, serialize...
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

        tsg.setStorageBytes(maxTimeDiffMs, maxTimeDiffMs);

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

        tsg.setStorageBytes(maxDataValueAfterDivision, minDataValueAfterDivision);

        return tsg;
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

   
    public static byte getOffsetBytes(byte[] payload) {
        return payload[OFFSET_BYTECOUNT_LOCATION];
    }

    public static int getOffsetDecimals(byte[] payload) {
        return TIME_GRANULARITY[payload[OFFSET_DECIMALCOUNT_LOCATION]];
    }

    public static byte getGranularityBytes(byte[] payload) {
        return payload[PAYLOAD_SIZE_IN_BYTES_LOCATION];
    }

    public static int getGranularityDivisor(byte[] payload) {
        return DATA_GRANULARITY[payload[PAYLOAD_DIVISOR_SIZE_LOCATION]];
    }

    public int getPayloadSize(byte[] payload) {
        return payload.length;
    }

    public static byte getGranularityDecimals(byte[] payload) {
        return payload[DECIMAL_PLACES_LOCATION];
    }

}
