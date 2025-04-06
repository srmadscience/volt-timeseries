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

import java.math.BigDecimal;
import java.util.Date;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import ie.voltdb.timeseries.CompressedTimeSeries;

class TestExpandWithMinus2DecimalPlaces {

    CompressedTimeSeries t = new CompressedTimeSeries((byte) -2);

    final Date zeroDate = new Date(0);
    @SuppressWarnings("deprecation")
    final Date startDate = new Date(124, 6, 21, 9, 03);
    final BigDecimal TEST_VALUE1 = new BigDecimal("4200");

    @SuppressWarnings("deprecation")
    final Date middleDate = new Date(124, 6, 21, 9, 04);
    final BigDecimal TEST_VALUE2 = new BigDecimal("4300");

    @SuppressWarnings("deprecation")
    final Date endDate = new Date(124, 6, 21, 9, 05);
    final BigDecimal TEST_VALUE3 = new BigDecimal("4400");

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
    void testExpandOneRow() {

        VoltTable table = new VoltTable(new VoltTable.ColumnInfo("id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("batch_date", VoltType.TIMESTAMP),
                new VoltTable.ColumnInfo("ts", VoltType.VARBINARY),
                new VoltTable.ColumnInfo("ANUMBER", VoltType.BIGINT));

        table.addRow(1, startDate, t.toBytes(), 42);
        // table.addRow(2, middleDate, t.toBytes(), 43);
        // table.addRow(3, endDate, t.toBytes(), 44);

        System.out.println(table.toFormattedString());

        VoltTable table2 = CompressedTimeSeries.expand(table, "ts");

        System.out.println(table2.toFormattedString());

        assertEquals(table2.getRowCount(), 3);

    }

    @Test
    void testExpandThreeRows() {

        VoltTable table = new VoltTable(new VoltTable.ColumnInfo("id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("batch_date", VoltType.TIMESTAMP),
                new VoltTable.ColumnInfo("ts", VoltType.VARBINARY),
                new VoltTable.ColumnInfo("ANUMBER", VoltType.BIGINT));

        table.addRow(1, startDate, t.toBytes(), 42);
        table.addRow(2, middleDate, t.toBytes(), 143);
        table.addRow(3, endDate, t.toBytes(), 244);

        System.out.println(table.toFormattedString());

        VoltTable table2 = CompressedTimeSeries.expand(table, "ts");

        System.out.println(table2.toFormattedString());

        assertEquals(table2.getRowCount(), 9);

    }

    @Test
    void testExpand100Rows() {

        CompressedTimeSeries t2 = new CompressedTimeSeries((byte) -2);

        final long startMs = System.currentTimeMillis();

        for (int i = 0; i < 100; i++) {
            t2.put(new Date(startMs + i), i % 10);
        }

        VoltTable table = new VoltTable(new VoltTable.ColumnInfo("id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("batch_date", VoltType.TIMESTAMP),
                new VoltTable.ColumnInfo("ts", VoltType.VARBINARY),
                new VoltTable.ColumnInfo("ANUMBER", VoltType.BIGINT));

        table.addRow(1, startDate, t2.toBytes(), 42);

        System.out.println(table.toFormattedString());

        VoltTable table2 = CompressedTimeSeries.expand(table, "ts");

        System.out.println(table2.toFormattedString());

        assertEquals(table2.getRowCount(), 100);

    }

}
