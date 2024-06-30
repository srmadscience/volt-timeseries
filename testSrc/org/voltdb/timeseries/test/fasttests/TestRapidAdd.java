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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Date;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import ie.voltdb.timeseries.CompressedTimeSeries;
import ie.voltdb.timeseries.TimeSeries;

class TestRapidAdd {

    CompressedTimeSeries t = new CompressedTimeSeries();

    final Date zeroDate = new Date(0);
    @SuppressWarnings("deprecation")
    final Date startDate = new Date(124, 6, 21, 9, 03);
    final long TEST_VALUE1 = 42;

    @SuppressWarnings("deprecation")
    final Date middleDate = new Date(124, 6, 21, 9, 04);
    final long TEST_VALUE2 = 43;

    @SuppressWarnings("deprecation")
    final Date endDate = new Date(124, 6, 21, 9, 05);
    final long TEST_VALUE3 = 44;

    @SuppressWarnings("deprecation")
    final Date afterEndDate = new Date(124, 6, 21, 9, 06);
    final long TEST_VALUE4 = 45;

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
    void testNullOp() {

        byte[] anArray = t.toBytes();

        byte[] sameArray = CompressedTimeSeries.put(anArray, endDate, TEST_VALUE3);
        assertArrayEquals(anArray, sameArray);

    }

    @Test
    void testLaterDateSameValue() {

        byte[] anArray = t.toBytes();

        t.put(afterEndDate, TEST_VALUE3);

        byte[] newArray = t.toBytes();

        byte[] newerArray = CompressedTimeSeries.put(anArray, afterEndDate, TEST_VALUE3);
        
        byte[] newArrayFirstBit = new byte[newArray.length - TimeSeries.TRAILING_DATE_BYTES];
        byte[] newerArrayFirstBit = new byte[newerArray.length - TimeSeries.TRAILING_DATE_BYTES];
        
        System.arraycopy(newArray,0 , newArrayFirstBit, 0, newArray.length - TimeSeries.TRAILING_DATE_BYTES);
        System.arraycopy(newerArray,0 , newerArrayFirstBit, 0, newerArray.length - TimeSeries.TRAILING_DATE_BYTES);
        
        assertArrayEquals(newArrayFirstBit, newerArrayFirstBit);

    }
    
    @Test
    void testLaterDateDiffrentValue() {

        byte[] anArray = t.toBytes();

        t.put(afterEndDate, TEST_VALUE4);

        byte[] newArray = t.toBytes();

        byte[] newerArray = CompressedTimeSeries.put(anArray,afterEndDate, TEST_VALUE4);
        assertArrayEquals(newerArray, newArray);

    }
    
    @Test
    void testFirstEntryUsing3ElementArray() {

        byte[] referenceArray = t.toBytes();
        
        byte[] nullArray = null;
        byte[] startedAsNullArray = CompressedTimeSeries.put(nullArray,startDate, TEST_VALUE1);
        startedAsNullArray = CompressedTimeSeries.put(startedAsNullArray,middleDate, TEST_VALUE2);
        startedAsNullArray = CompressedTimeSeries.put(startedAsNullArray,endDate, TEST_VALUE3);
        
        assertArrayEquals(referenceArray, startedAsNullArray);

    }
    
    @Test
    void testFirstEntryUsing1ElementArray() {

        
        CompressedTimeSeries t2 = new CompressedTimeSeries();
        t2.put(startDate, TEST_VALUE1);
        
        byte[] referenceArray = t2.toBytes();
        
        byte[] nullArray = null;
        byte[] startedAsNullArray = CompressedTimeSeries.put(nullArray,startDate, TEST_VALUE1);
        
        assertArrayEquals(referenceArray, startedAsNullArray);

    }

}
