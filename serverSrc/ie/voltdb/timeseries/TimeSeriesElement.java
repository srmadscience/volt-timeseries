/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
package ie.voltdb.timeseries;

import java.math.BigDecimal;
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
