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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ie.voltdb.timeseries.BigDecimalHasWrongScaleException;
import ie.voltdb.timeseries.CompressedTimeSeries;
import ie.voltdb.timeseries.TimeSeries;
import ie.voltdb.timeseries.TimeSeriesElement;

class Test3ElementCompressedTimeSeries2Decimals {

    CompressedTimeSeries t = new CompressedTimeSeries((byte) 2);

  
    final Date zeroDate = new Date(0);
    @SuppressWarnings("deprecation")
    final Date startDate = new Date(124, 6, 21, 9, 03);
    final BigDecimal TEST_VALUE1 = new BigDecimal("42.00");

    @SuppressWarnings("deprecation")
    final Date middleDate = new Date(124, 6, 21, 9, 04);
    final BigDecimal TEST_VALUE2 = new BigDecimal("43.00");

    @SuppressWarnings("deprecation")
    final Date endDate = new Date(124, 6, 21, 9, 05);
    final BigDecimal TEST_VALUE3 = new BigDecimal("44.00");

    @BeforeAll
    static void setUpBeforeClass() throws Exception {

    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
    }

    @BeforeEach
    void setUp() throws Exception {

        t.put(startDate, TEST_VALUE1);
        t.put(endDate, TEST_VALUE3);
        t.put(middleDate, TEST_VALUE2);
    }

    @AfterEach
    void tearDown() throws Exception {
    }

    @Test
    void testGet() {
        
            BigDecimal l;
            try {
                l = t.findValueForFirstLocationEqualOrAfterBigDecimal(zeroDate);
                assertEquals(l,TEST_VALUE1, "testGet shoudl return reference item");
            } catch (BigDecimalHasWrongScaleException e) {
                fail();
            }
       
    }

    @Test
    void testFind() {

        BigDecimal l;
        try {
            l = t.findValueForExactMatchBigDecimal(new Date(0));
            assertNull(l,  "all searches should return -1 for null t");
        } catch (BigDecimalHasWrongScaleException e) {
            fail();
        }

    }

   

    @Test
    void testToArray() {

        TimeSeriesElement[] theArray = t.toArray();

        assertEquals(theArray.length, 3);

        assertEquals(true, arrayOK(theArray));

        Date[] testDates = { startDate, middleDate, endDate };

        for (int i = 0; i < testDates.length; i++) {
            long l = t.findExactMatchLocation(testDates[i]);
            assertEquals(l, i, "Expected Value Not Returned " + l + " " + i);

        }

    }

    @Test
    void testToBytes() {

        byte[] theArray = t.toBytes();
        CompressedTimeSeries t2 = new CompressedTimeSeries(theArray);

        System.out.println(t.toString());
        System.out.println(t2.toString());

        // assertEquals(t2.toString(),t.toString(), "Object mutating");

        byte[] theArray2 = t2.toBytes();
        assertArrayEquals(theArray, theArray2);

    }

    @Test
    void testGetMinDate() {

        Date foo = t.getMinTime();
        assertEquals(foo, startDate);

    }

    @Test
    void testGetMaxDate() {

        Date foo = t.getMaxTime();
        assertEquals(foo, endDate);

    }



    public static boolean arrayOK(TimeSeriesElement[] theArray) {

        Date lastDate = new Date(0);
        long lastValue = Long.MIN_VALUE;

        for (TimeSeriesElement element : theArray) {

            if (element.getEventTime().before(lastDate) || (element.getEventTime().getTime() == lastDate.getTime())) {
                System.err.println("arrayOK: " + element.getEventTime() + " " + lastDate);
                return false;
            }

            if (element.getValue() < lastValue) {
                System.err.println("arrayOK: " + element.getValue() + " " + lastValue);
                return false;
            }

            lastDate = element.getEventTime();
            lastValue = element.getValue();

        }

        return true;
    }

}
