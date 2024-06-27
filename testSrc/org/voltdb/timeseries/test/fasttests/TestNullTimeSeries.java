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
package org.voltdb.timeseries.test.fasttests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Date;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ie.voltdb.timeseries.CompressedTimeSeries;
import ie.voltdb.timeseries.TimeSeries;
import ie.voltdb.timeseries.TimeSeriesElement;

class TestNullTimeSeries {

    TimeSeries t = new TimeSeries();

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
    }

    @BeforeEach
    void setUp() throws Exception {
    }

    @AfterEach
    void tearDown() throws Exception {
    }

    @Test
    void testGet() {

        long l = t.getValue(new Date(0));
        assertEquals(l,-1,"all searches should return -1 for null t");


    }

    @Test
    void testFind() {

        long l =  t.findExactMatch(new Date(0));
        assertEquals(l,-1,"all searches should return -1 for null t");


    }

    @Test
    void testToBytes() {

        byte offsetBytes = Long.BYTES;
        byte offsetDecimals = (byte) (CompressedTimeSeries.TIME_GRANULARITY.length -1);
        byte payloadBytes = Long.BYTES;
        byte payloadDecimals = (byte) (CompressedTimeSeries.DATA_GRANULARITY.length -1);

        byte[] metadata = { offsetBytes, offsetDecimals, payloadBytes, payloadDecimals };

        byte[] ourBytes = t.toBytes();

        Assertions.assertArrayEquals(metadata,ourBytes,"toBytes should return metadata for null t");
    }

    @Test
     void testToString() {

        String expected = "TimeSeries [minTime=, maxTime=, minValue=9223372036854775807, maxValue=-9223372036854775808, timeData=null]";

        String foo = t.toString();
        assertEquals(foo,expected,"toBytes should return known string for null t");


    }

    @Test
    void testToArray() {

        TimeSeriesElement[] theArray = t.toArray();

        assertNull(theArray);

   }

    @Test
    void testGetMinDate() {

       Date foo = t.getMinTime();
       assertEquals(foo,null);


   }



    @Test
    void testGetMaxDate() {

       Date foo = t.getMaxTime();
       assertEquals(foo,null);


   }
    @Test
    void testGetMinValue() {

       long foo = t.getMinValue();
       assertEquals(foo,Long.MAX_VALUE);


   }


    @Test
    void testGetMaxValue() {

       long foo = t.getMaxValue();
       assertEquals(foo,Long.MIN_VALUE);


   }





}
