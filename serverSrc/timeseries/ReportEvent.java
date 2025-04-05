/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
package timeseries;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;


/**
 * Return information about a session
 */
public class ReportEvent extends VoltProcedure {

	// @formatter:off

    public static final String TRUNC_INTERVAL = "MINUTE";

	public static final SQLStmt addUncompressed = new SQLStmt(
			"upsert into normal_timeseries_table values (?,?,?);");

    public static final SQLStmt updateCompressed = new SQLStmt(
            "update compressed_timeseries_table set event_ts = VoltTimeSeriesput(event_ts,?,?) where message_type_id = ? and message_time = TRUNCATE("+TRUNC_INTERVAL+",?);");

    public static final SQLStmt addCompressed = new SQLStmt(
            "insert into compressed_timeseries_table values (?,TRUNCATE("+TRUNC_INTERVAL+",?), VoltTimeSeriesputFirst(?,?));");

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
