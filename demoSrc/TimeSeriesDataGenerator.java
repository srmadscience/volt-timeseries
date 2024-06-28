
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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.topics.VoltDBKafkaPartitioner;

/**
 * This generates mock CDRS that need to be aggregated. It also deliberately
 * introduces the same kind of mistakes we see in the real world, such as
 * duplicate records, missing records and late records.
 *
 */
public class TimeSeriesDataGenerator {

    Client voltClient = null;

    String hostnames;
    String nessageType;
    int tpMs;
    int runSeconds;
    long maxValue;
    int changeInterval;
    boolean deleteOld = false;
    int timeChangeInterval;

    public TimeSeriesDataGenerator(String hostnames, String nessageType, int tpMs, int runSeconds, long maxValue,
            int changeInterval, int deleteOldInt, int timeChangeInterval) {
        super();
        this.hostnames = hostnames;
        this.nessageType = nessageType;
        this.tpMs = tpMs;
        this.runSeconds = runSeconds;
        this.maxValue = maxValue;
        this.changeInterval = changeInterval;
        this.timeChangeInterval =  timeChangeInterval;
        
        if (deleteOldInt > 0) {
            deleteOld = true;
        }

        try {
            voltClient = connectVoltDB(hostnames);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public boolean run() {

        Random r = new Random();
        long startMs = System.currentTimeMillis();

        long currentMs = System.currentTimeMillis();
        int tpThisMs = 0;
        long recordCount = 0;

        long value = r.nextLong(maxValue);
        int sameInterval = 0; if (deleteOld) {
        try {
            voltClient.callProcedure("@AdHoc", "DELETE FROM NORMAL_TIMESERIES_TABLE;");
            voltClient.callProcedure("@AdHoc", "DELETE FROM COMPRESSED_TIMESERIES_TABLE;");
        } catch (IOException | ProcCallException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        }

        while (System.currentTimeMillis() < (startMs + (1000 * runSeconds))) {

            recordCount++;

            if (++sameInterval == changeInterval) {
                value = r.nextLong(maxValue);
                sameInterval = 0;
            }

            ComplainOnErrorCallback coec = new ComplainOnErrorCallback();

            try {
                voltClient.callProcedure(coec, "ReportEvent", nessageType, new Date(startMs + recordCount), value);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            if (tpThisMs++ > tpMs) {

                // but sleep if we're moving too fast...
                while (currentMs == System.currentTimeMillis()) {
                    try {
                        Thread.sleep(0, 50000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                currentMs = System.currentTimeMillis();
                tpThisMs = 0;
            }

            if (recordCount % 10000 == 0) {
                msg("Record " + recordCount + "...");
            }

        }

        try {
            ClientResponse cr = voltClient.callProcedure("GetEvents", nessageType, new Date(startMs),
                    new Date(startMs + recordCount));
            msg(cr.getResults()[0].toFormattedString());
            
            
            cr = voltClient.callProcedure("@Statistics", "TABLE", 0);

            long compressedSize = 0;
            long unCompressedSize = 0;
            long compressedRows = 0;
            long unCompressedRows = 0;

            while (cr.getResults()[0].advanceRow()) {

                String tableName = cr.getResults()[0].getString("TABLE_NAME");

                if (tableName.equalsIgnoreCase("COMPRESSED_TIMESERIES_TABLE")) {
                    compressedSize += cr.getResults()[0].getLong("TUPLE_ALLOCATED_MEMORY");
                    compressedSize += cr.getResults()[0].getLong("STRING_DATA_MEMORY");
                    compressedRows += cr.getResults()[0].getLong("TUPLE_COUNT");
                } else if (tableName.equalsIgnoreCase("NORMAL_TIMESERIES_TABLE")) {
                    unCompressedSize += cr.getResults()[0].getLong("TUPLE_ALLOCATED_MEMORY");
                    unCompressedSize += cr.getResults()[0].getLong("STRING_DATA_MEMORY");
                 unCompressedRows += cr.getResults()[0].getLong("TUPLE_COUNT");
                }

            }

           // msg(cr.getResults()[0].toFormattedString());
            msg("Uncompressed size/rows = " + unCompressedSize + "/" + unCompressedRows);
            msg("compressed   size/rows = " + compressedSize + "/" + compressedRows);

            if (compressedSize > 0) {
                msg("compression ratio = " + (unCompressedSize / compressedSize));
            }

            
            msg("GREPME:"+hostnames+":"+nessageType+":"+tpMs+":"+runSeconds+":"+maxValue+":"+
                changeInterval+":"+deleteOld+":"+unCompressedSize + ":" + unCompressedRows+ ":" +compressedSize + ":" + compressedRows+":"+recordCount);
        } catch (IOException | ProcCallException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            voltClient.drain();
            voltClient.close();
        } catch (NoConnectionsException | InterruptedException e) {
            e.printStackTrace();
        }

        return true;

    }

    /**
     *
     * Connect to VoltDB using native APIS
     *
     * @param commaDelimitedHostnames
     * @return
     * @throws Exception
     */
    private static Client connectVoltDB(String commaDelimitedHostnames) throws Exception {
        Client client = null;
        ClientConfig config = null;

        try {
            msg("Logging into VoltDB");

            config = new ClientConfig(); // "admin", "idontknow");
            config.setTopologyChangeAware(true);
            // config.setReconnectOnConnectionLoss(true);
            config.setHeavyweight(true);

            client = ClientFactory.createClient(config);

            String[] hostnameArray = commaDelimitedHostnames.split(",");

            for (String element : hostnameArray) {
                msg("Connect to " + element + "...");
                try {
                    client.createConnection(element);
                } catch (Exception e) {
                    msg(e.getMessage());
                }
            }

            if (client.getConnectedHostList().size() > 0) {
                msg("Connected to Volt");
            } else {
                msg("Warning: Not Connected to Volt");
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Volt connection failed.." + e.getMessage(), e);
        }

        return client;

    }

    /**
     * Print a formatted message.
     *
     * @param message
     */
    public static void msg(String message) {

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);

    }

    public static void main(String[] args) throws Exception {

        msg("Parameters:" + Arrays.toString(args));

        if (args.length != 8) {
            msg("Usage: hostnames messagetype tpms durationseconds maxvalue changeinterval deleteOld_zero_or_one timechangeinterval");
            System.exit(1);
        }

        String hostnames = args[0];
        String nessageType = args[1];
        int tpMs = Integer.parseInt(args[2]);
        int runSeconds = Integer.parseInt(args[3]);
        long maxValue = Long.parseLong(args[4].replace(",", ""));
        
        int changeInterval = Integer.parseInt(args[5]);
        int deleteOldInt = Integer.parseInt(args[6]);
        int timechangeinterval = Integer.parseInt(args[7]);

        TimeSeriesDataGenerator g = new TimeSeriesDataGenerator(hostnames, nessageType, tpMs, runSeconds, maxValue,
                changeInterval,deleteOldInt,timechangeinterval);

        g.run();

    }
}
