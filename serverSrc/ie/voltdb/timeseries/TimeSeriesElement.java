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

public class TimeSeriesElement {

    private Date eventTime;
    private long value;

    public TimeSeriesElement(Date eventTime, long value) {
        super();
        this.eventTime = eventTime;
        this.value = value;
    }

    public boolean after(Date otherEventTime) {


        return eventTime.after(otherEventTime);

    }

    public boolean before(Date otherEventTime) {

        return eventTime.before(otherEventTime);
    }

    public boolean equals(Date otherEventTime) {

        return otherEventTime.equals(eventTime);


    }



    public long getTimeOffsetMS (Date earlierEvent) {
        return Math.abs(eventTime.getTime() - earlierEvent.getTime());
    }

    public long getValue() {
        return value;
    }

    /**
     * @return the eventTime
     */
    public Date getEventTime() {
        return eventTime;
    }

    /**
     * @param value the value to set
     */
    public void setValue(long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("TimeSeriesElement [eventTime=");
        builder.append(eventTime.toGMTString());
        builder.append(", value=");
        builder.append(value);
        builder.append("]");
        return builder.toString();
    }

    /**
     * @param eventTime the eventTime to set
     */
    public void setEventTime(Date eventTime) {
        this.eventTime = eventTime;
    }



}
