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
