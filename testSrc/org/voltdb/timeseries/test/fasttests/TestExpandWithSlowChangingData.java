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

import java.util.Date;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import ie.voltdb.timeseries.CompressedTimeSeries;

class TestExpandWithSlowChangingData {

    CompressedTimeSeries t = new CompressedTimeSeries();

    @BeforeAll
    static void setUpBeforeClass() throws Exception {

    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
    }

    @BeforeEach
    void setUp() throws Exception {

        final long startMs = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {
            t.put(new Date(startMs + i), i / 200);
        }

    }

    @AfterEach
    void tearDown() throws Exception {
    }

    @Test
    void testExpand() {

        VoltTable table = new VoltTable(new VoltTable.ColumnInfo("id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("batch_date", VoltType.TIMESTAMP),
                new VoltTable.ColumnInfo("ts", VoltType.VARBINARY),
                new VoltTable.ColumnInfo("ANUMBER", VoltType.BIGINT));

        table.addRow(1, new Date(), t.toBytes(), 42);

        System.out.println(table.toFormattedString());

        VoltTable table2 = CompressedTimeSeries.expand(table, "ts");

        System.out.println(table2.toFormattedString());

        assertEquals(table2.getRowCount(), 5);

    }

}
