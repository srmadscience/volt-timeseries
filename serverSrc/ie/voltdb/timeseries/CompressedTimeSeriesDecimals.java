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

import java.util.Date;

/**
 * 
 */
public class CompressedTimeSeriesDecimals extends CompressedTimeSeries {

    byte decimalplaces;
    
    public CompressedTimeSeriesDecimals(byte decimalplaces) {
        super();
        this.decimalplaces = decimalplaces;
    }

  
    /**
     * @param payload
     */
    public CompressedTimeSeriesDecimals(byte[] payload,byte decimalplaces) {
        super(payload);
        this.decimalplaces = decimalplaces;
    }


    @Override
    public boolean put(Date eventTime, long value) {
        if (decimalplaces == 0) {
            return super.put(eventTime, value);
        }
        
        return super.put(eventTime, (long) (value * Math.pow(10, decimalplaces)));
        
    }


 
    @Override
    public long findValueForExactMatch(Date aTime) {
        return  (long) (super.findValueForExactMatch(aTime) / Math.pow(10, decimalplaces));
    }


    @Override
    public long findValueForFirstElementEqualOrAfter(Date aTime) {
        return (long) (super.findValueForFirstElementEqualOrAfter(aTime) / Math.pow(10, decimalplaces));
    }


    @Override
    public long getValue(Date referenceTime) {
        return (long) (super.getValue(referenceTime) / Math.pow(10, decimalplaces));
    }


 
    @Override
    public long getMinValue() {
        return (long) (super.getMinValue() / Math.pow(10, decimalplaces));
    }


    @Override
    public long getMaxValue() {
        return (long) (super.getMaxValue() / Math.pow(10, decimalplaces));
    }


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("CompressedTimeSeriesDecimals [decimalplaces=");
        builder.append(decimalplaces);
        builder.append(", minTime=");
        if (minTime != null) {
            builder.append(minTime.toGMTString());
        }
        builder.append(", maxTime=");
        if (maxTime != null) {
            builder.append(maxTime.toGMTString());
        }
        builder.append(", minValue=");
        builder.append(minValue);
        builder.append(", maxValue=");
        builder.append(maxValue);
        builder.append(", minAndMaxValueAreUnreliable=");
        builder.append(minAndMaxValueAreUnreliable);
        builder.append(", timeData=");
        builder.append(timeData);
        builder.append("]");
        return builder.toString();
    }

}
