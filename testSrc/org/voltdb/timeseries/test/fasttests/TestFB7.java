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

import java.util.Date;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ie.voltdb.timeseries.BigDecimalHasWrongScaleException;
import ie.voltdb.timeseries.CompressedTimeSeries;
import ie.voltdb.timeseries.TimeSeriesElement;

class TestFB7 {

    CompressedTimeSeries t = new CompressedTimeSeries();

  
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
    void testFB7() {
        try {
            byte[] payload = { 2, 6, 1, 11, 0, 0, 0, 1, -106, 26, -94, -48, -128, 0, 0, 2, 0, -99, 2, 0, 0, 12, 0, 100, 2, 1, 44, 1, 0, 100, 9, 0, -56, 3, 0, 100, 8, 2, 88, 15, 2, 88, 7, 3, 32, 10, 1, 44, 12, 1, 44, 3, 0, -56, 6, 1, 44, 4, 1, 44, 2, 1, -12, 7, 0, -56, 5, 3, 32, 14, 0, 0, 1, -106, 26, -93, -75, 75};
            CompressedTimeSeries cts = new CompressedTimeSeries(payload);
         } catch (Exception e) {
            fail();
        }
    }

    

  
 

}
