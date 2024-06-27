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


/**
 * Return information about a session
 */
public class ReportEvent extends VoltProcedure {

	// @formatter:off

	public static final SQLStmt addUncompressed = new SQLStmt(
			"upsert into normal_timeseries_table values (?,?,?);");
         
    public static final SQLStmt updateCompressed = new SQLStmt(
            "update compressed_timeseries_table set event_ts = VoltTimeSeriesput(event_ts,?,?) where message_type_id = ? and message_hour = TRUNCATE(HOUR,?);");
            
    public static final SQLStmt addCompressed = new SQLStmt(
            "insert into compressed_timeseries_table values (?,TRUNCATE(HOUR,?), VoltTimeSeriesputFirst(?,?));");

    // @formatter:on

	public VoltTable[] run(String messageTypeId, TimestampType eventTime, long eventValue) throws VoltAbortException {

		voltQueueSQL(addUncompressed,messageTypeId,  eventTime,  eventValue);
		voltQueueSQL(updateCompressed, eventTime, eventValue,messageTypeId,eventTime );
		
		VoltTable[] results = voltExecuteSQL();
		
		results[1].advanceRow();
		if (results[1].getLong(0) == 0) {
		    voltQueueSQL(addCompressed, messageTypeId,eventTime, eventTime,eventValue) ;
		}

		return voltExecuteSQL(true);

	}

}
