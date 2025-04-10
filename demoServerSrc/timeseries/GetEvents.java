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

		if (results[0] == null) {
		    return null;
		}

		return results;

	}

}
