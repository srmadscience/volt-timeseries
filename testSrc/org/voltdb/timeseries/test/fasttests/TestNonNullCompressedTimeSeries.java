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

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Date;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ie.voltdb.timeseries.BigDecimalHasWrongScaleException;
import ie.voltdb.timeseries.TimeSeries;
import ie.voltdb.timeseries.TimeSeriesElement;

class TestNonNullCompressedTimeSeries {

    TimeSeries t = new TimeSeries();

    final Date zeroDate = new Date(0);
    @SuppressWarnings("deprecation")
    final Date startDate = new Date(124, 6, 21, 9, 03);
    final long TEST_VALUE = 42;

    @BeforeAll
    static void setUpBeforeClass() throws Exception {

    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
    }

    @BeforeEach
    void setUp() throws Exception {

        t.put(startDate, TEST_VALUE);
    }

    @AfterEach
    void tearDown() throws Exception {
    }

    @Test
    void testGet() {

        try {
            long l = t.findValueForExactMatch(startDate);
            assertEquals(l, TEST_VALUE, "testGet shoudl return reference item");
        } catch (BigDecimalHasWrongScaleException e) {
            fail();
        }

    }

    @Test
    void testFind() {

        long l = t.findExactMatchLocation(new Date(0));
        assertEquals(l, Integer.MIN_VALUE, "all searches should return -1 for null t");

    }

    @Test
    void testToBytes() {

        byte[] expectedData = {8, 7, 8, 14, 0, 0, 0, 1, -112, -44, 80, -65, 32, 0, 0, 1, -112, -44, 80
                , -65, 32, 0, 0, 0, 0, 0, 0, 0, 42 };

        byte[] ourBytes = t.toBytes();

        Assertions.assertArrayEquals(expectedData, ourBytes, "toBytes should return metadata for null t");
    }

    @Test
    void testToString() {

        String expected = "TimeSeries [minTime=21 Jul 2024 08:03:00 GMT, maxTime=21 Jul 2024 08:03:00 GMT, minValue=42, maxValue=42, decimalPlaces=0, timeData=[TimeSeriesElement [eventTime=21 Jul 2024 08:03:00 GMT, value=42]]]";

        String foo = t.toString();
        assertEquals(foo, expected, "toBytes should return known string for null t");

    }

    @Test
    void testToArray() {

        TimeSeriesElement[] theArray = t.toArray();

        assertEquals(theArray.length, 1);

    }

    @Test
    void testConstructor() {

        byte[] inputData = { 8, 7, 8, 14, 0, 0, 0, 56, 24, -20, 48, -97
                , 32, 0, 0, 56, 24, -20, 48, -97, 32, 0, 0, 0, 0, 0, 0, 0, 42 };

        TimeSeries t2 = new TimeSeries(inputData);

        byte[] ourBytes = t2.toBytes();

        Assertions.assertArrayEquals(inputData, ourBytes);
    }

    @Test
    void testGetMinDate() {

        Date foo = t.getMinTime();
        assertEquals(foo, startDate);

    }

    @Test
    void testGetMaxDate() {

        Date foo = t.getMaxTime();
        assertEquals(foo, startDate);

    }

    @Test
    void testGetMinValue() {

        long foo = t.getMinValue();
        assertEquals(foo, 42);

    }

    @Test
    void testGetMaxValue() {

        long foo = t.getMaxValue();
        assertEquals(foo, 42);

    }

}
