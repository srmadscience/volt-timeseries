/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
package ie.voltdb.timeseries;

public class TimeSeriesGranularity {

    private byte storageBytes = Long.BYTES;
    private byte divisorId = 0;

    /**
     * @return the storageBytes
     */
    public byte getStorageBytes() {
        return storageBytes;
    }

    /**
     * @param storageBytes the storageBytes to set
     */
    public void setStorageBytes(byte storageBytes) {
        this.storageBytes = storageBytes;
    }

    /**
     * @return the trailingZeros
     */
    public byte getDivisorId() {
        return divisorId;
    }

    /**
     * @param trailingZeros the trailingZeros to set
     */
    public void setDivisorId(byte newDivisorId) {
        this.divisorId = newDivisorId;
    }

    /**
     * @param maxDataValueAfterDivision
     * @param minDataValueAfterDivision
     */
    public  void setStorageBytes(long maxDataValueAfterDivision,
            long minDataValueAfterDivision) {
        if (maxDataValueAfterDivision <= Byte.MAX_VALUE && minDataValueAfterDivision >= Byte.MIN_VALUE) {
            setStorageBytes((byte) Byte.BYTES);
        } else if (maxDataValueAfterDivision <= Short.MAX_VALUE && minDataValueAfterDivision >= Short.MIN_VALUE) {
            setStorageBytes((byte) Short.BYTES);
        } else if (maxDataValueAfterDivision <= Integer.MAX_VALUE && minDataValueAfterDivision >= Integer.MIN_VALUE) {
            setStorageBytes((byte) Integer.BYTES);
        } else {
            setStorageBytes((byte) Long.BYTES);
        }
    }



    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("TimeSeriesGranularity [storageBytes=");
        builder.append(storageBytes);
        builder.append(", divisorId=");
        builder.append(divisorId);
        builder.append("]");
        return builder.toString();
    }

}
