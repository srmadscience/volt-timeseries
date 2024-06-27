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
package org.voltdb.timeseries.test.slowtests;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import ie.voltdb.timeseries.CompressedTimeSeries;
import ie.voltdb.timeseries.TimeSeriesElement;

class TestManyElementCompressedTimeSeries {

    private static final int TEST_SIZE = 50000000;

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
    void testMany() {

        t = new CompressedTimeSeries();
        Random r = new Random(42);
        final Date start = new Date();
        final int msDuration = 1000 * 3600;

        for (int i = 0; i < TEST_SIZE; i++) {
            Date thisDate = new Date(start.getTime() + r.nextInt(msDuration));
            long value = thisDate.getTime() - start.getTime();
            t.put(thisDate, value);

            TimeSeriesElement[] theArray = t.toArray();

            assertEquals(true, arrayOK(theArray));

            if (i % 1 == 100) {
                System.out.println("testManyRandomLons " + i);
            }
            if (r.nextInt(100) == 0) {
                byte[] asBytes = t.toBytes();
                CompressedTimeSeries t3 = new CompressedTimeSeries(asBytes);
                byte[] asBytesT3 = t3.toBytes();

                if (!Arrays.equals(asBytes, asBytesT3)) {
                    System.out.println(t.toString());
                    System.out.println(t3.toString());
                }
                assertArrayEquals(asBytes, asBytesT3);
                // System.out.println("Compressed Event " + i + " " + t.size() + " "+
                // (asBytes.length / (1024))+ "KB");

                if (asBytes.length / (1024) > 1024) {
                    System.out.println("Compressed Ended at " + i + " " + t.size() + " " + (asBytes.length / (1024))
                            + "KB after " + (System.currentTimeMillis() - start.getTime() + "ms"));
                    break;
                }

            }

        }

    }

    @Test
    void testManyRandomInts() {

        t = new CompressedTimeSeries();
        Random r = new Random(37);
        final Date start = new Date();
        final int msDuration = 1000 * 3600;

        for (int i = 0; i < TEST_SIZE; i++) {
            Date thisDate = new Date(start.getTime() + r.nextInt(msDuration));
            long value = r.nextInt();
            t.put(thisDate, value);

            TimeSeriesElement[] theArray = t.toArray();

            assertEquals(true, arrayOK(theArray), "element " + i);

            if (i % 1 == 100000) {
                System.out.println("testManyRandomInts " + i);
            }

            if (r.nextInt(100) == 0) {
                byte[] asBytes = t.toBytes();
                CompressedTimeSeries t3 = new CompressedTimeSeries(asBytes);
                byte[] asBytesT3 = t3.toBytes();

                if (!Arrays.equals(asBytes, asBytesT3)) {
                    System.out.println(t.toString());
                    System.out.println(t3.toString());
                }
                assertArrayEquals(asBytes, asBytesT3);

                if (asBytes.length / (1024) > 1024) {
                    System.out.println("Compressed testManyRandomInts Ended at " + i + " " + t.size() + " "
                            + (asBytes.length / (1024)) + "KB after "
                            + (System.currentTimeMillis() - start.getTime() + "ms"));
                    break;
                }
            }

        }

    }

    @Test
    void testManyRandomShorts() {

        t = new CompressedTimeSeries();
        Random r = new Random(100);
        final Date start = new Date();
        final int msDuration = 1000 * 3600;

        for (int i = 0; i < TEST_SIZE; i++) {
            Date thisDate = new Date(start.getTime() + r.nextInt(msDuration));
            long value = r.nextInt(Short.MAX_VALUE);
            t.put(thisDate, value);

            TimeSeriesElement[] theArray = t.toArray();
            assertEquals(true, arrayOK(theArray), "element " + i);

            if (i % 1 == 0) {
                System.out.println("testManyRandomShorts " + i);
            }

            if (r.nextInt(100) == 0) {

                byte[] asBytes = t.toBytes();
                CompressedTimeSeries t3 = new CompressedTimeSeries(asBytes);
                byte[] asBytesT3 = t3.toBytes();

                if (!Arrays.equals(asBytes, asBytesT3)) {
                    System.out.println(t.toString());
                    System.out.println(t3.toString());
                }
                assertArrayEquals(asBytes, asBytesT3);


                if (asBytes.length / (1024) > 1024) {
                    System.out.println("Compressed testManyRandomShorts Ended at " + i + " " + t.size() + " "
                            + (asBytes.length / (1024)) + "KB after"
                            + (System.currentTimeMillis() - start.getTime() + "ms"));
                    break;
                }
            }

        }

    }

    @Test
    void testManyRandomShortsOrdered() {

        t = new CompressedTimeSeries();
        Random r = new Random(100);
        final Date start = new Date();
        final int msDuration = 1000 * 3600;

        for (int i = 0; i < TEST_SIZE; i++) {
            Date thisDate = new Date(start.getTime() + i);
            long value = r.nextInt(Short.MAX_VALUE);
            t.put(thisDate, value);

            TimeSeriesElement[] theArray = t.toArray();
            assertEquals(true, arrayOK(theArray), "element " + i);

            if (i % 1 == 100000) {
                System.out.println("testManyRandomShorts " + i);
            }

            if (r.nextInt(100) == 0/* || true */) {
                byte[] asBytes = t.toBytes();
                CompressedTimeSeries t3 = new CompressedTimeSeries(asBytes);
                byte[] asBytesT3 = t3.toBytes();

                if (!Arrays.equals(asBytes, asBytesT3)) {
                    System.out.println(t.toString());
                    System.out.println(t3.toString());
                }
                assertArrayEquals(asBytes, asBytesT3);

                if (asBytes.length / (1024) > 1024) {
                    System.out.println("Compressed testManyRandomShorts Ended at " + i + " " + t.size() + " "
                            + (asBytes.length / (1024)) + "KB after"
                            + (System.currentTimeMillis() - start.getTime() + "ms"));
                    break;
                }
            }

        }

    }

    @Test
    void testManyRandomBytes() {

        t = new CompressedTimeSeries();
        Random r = new Random(132);
        final Date start = new Date();
        final int msDuration = 1000 * 3600;

        for (int i = 0; i < TEST_SIZE; i++) {
            Date thisDate = new Date(start.getTime() + r.nextInt(msDuration));
            long value = r.nextInt(Byte.MAX_VALUE);
            t.put(thisDate, value);

            TimeSeriesElement[] theArray = t.toArray();

            assertEquals(true, arrayOK(theArray), "element " + i);

            if (i % 1 == 10000) {
                System.out.println("testManyRandomBytes " + i);
            }

            if (r.nextInt(100) == 0) {
                byte[] asBytes = t.toBytes();
                CompressedTimeSeries t3 = new CompressedTimeSeries(asBytes);
                byte[] asBytesT3 = t3.toBytes();

                if (!Arrays.equals(asBytes, asBytesT3)) {
                    System.out.println(t.toString());
                    System.out.println(t3.toString());
                }
                assertArrayEquals(asBytes, asBytesT3);

                if (asBytes.length / (1024) > 1024) {
                    System.out.println("Compressed testManyRandomBytes Ended at " + i + " " + t.size() + " "
                            + (asBytes.length / (1024)) + "KB after"
                            + (System.currentTimeMillis() - start.getTime() + "ms"));
                    break;
                }
            }

        }

    }

    @Test
    void testManyOrderedBytes() {

        t = new CompressedTimeSeries();
        Random r = new Random(33);
        final Date start = new Date();
        final int msDuration = 1000 * 3600;

        for (int i = 0; i < TEST_SIZE; i++) {
            Date thisDate = new Date(start.getTime() + i);
            long value = r.nextInt(Byte.MAX_VALUE);
            t.put(thisDate, value);

            TimeSeriesElement[] theArray = t.toArray();

            assertEquals(true, arrayOK(theArray), "element " + i);

            if (i % 1 == 1000) {
                System.out.println("testManyOrderedBytes " + i);
            }

            if (r.nextInt(100) == 0) {
                byte[] asBytes = t.toBytes();
                CompressedTimeSeries t3 = new CompressedTimeSeries(asBytes);
                byte[] asBytesT3 = t3.toBytes();

                if (!Arrays.equals(asBytes, asBytesT3)) {
                    System.out.println(t.toString());
                    System.out.println(t3.toString());
                }
                assertArrayEquals(asBytes, asBytesT3);

                if (asBytes.length / (1024) > 1024) {
                    System.out.println("Compressed testManyOrderedBytes Ended at " + i + " " + t.size() + " "
                            + (asBytes.length / (1024)) + "KB after"
                            + (System.currentTimeMillis() - start.getTime() + "ms"));
                    break;
                }
            }

        }

    }

    public static boolean arrayOK(TimeSeriesElement[] theArray) {

        Date lastDate = new Date(0);
        long lastValue = Long.MIN_VALUE;

        for (TimeSeriesElement element : theArray) {

            if (element.getEventTime().before(lastDate) || (element.getEventTime().getTime() == lastDate.getTime())) {
                System.err.println("arrayOK: " + element.getEventTime() + " " + lastDate);
                return false;
            }

            lastDate = element.getEventTime();
            lastValue = element.getValue();

        }

        return true;
    }

}
