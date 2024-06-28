package timeseries;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2023 VoltDB Inc.
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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import ie.voltdb.timeseries.CompressedTimeSeries;


public class GetEvents extends VoltProcedure {

	// @formatter:off

        
    public static final String TRUNC_INTERVAL = "MINUTE";

    public static final SQLStmt getCompressed = new SQLStmt(
            "SELECT * from compressed_timeseries_table "
            + "WHERE message_type_id = ? "
            + "AND message_time BETWEEN TRUNCATE("+TRUNC_INTERVAL+",?) AND TRUNCATE("+TRUNC_INTERVAL+",?) "
            + "order by message_time;");
            
    // @formatter:on

	public VoltTable[] run(String messageTypeId, TimestampType startTime, TimestampType endTime) throws VoltAbortException {

		voltQueueSQL(getCompressed,messageTypeId,  startTime,  endTime);
		
		VoltTable[] results = voltExecuteSQL(true);
		
		results[0] = CompressedTimeSeries.expand(results[0], "EVENT_TS");

	
		return results;

	}

}
