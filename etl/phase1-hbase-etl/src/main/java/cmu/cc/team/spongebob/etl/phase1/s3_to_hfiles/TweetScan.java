package cmu.cc.team.spongebob.etl.phase1.s3_to_hfiles;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TweetScan extends Configured implements Tool {
    private static final Log LOGGER = LogFactory.getLog(HBaseETLJob.class);

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(HBaseConfiguration.create(), new TweetScan(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        long user1ID = Long.parseLong(args[0]);
        TableName tableName = TableName.valueOf(args[1]);

        try (Connection connection = ConnectionFactory.createConnection(getConf())) {

            try (Table table = connection.getTable(tableName)) {
                // Scan scan = new Scan(Bytes.toBytes(user1ID), Bytes.toBytes(user1ID + 1));
                // PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes(user1ID));
                // scan.setFilter(prefixFilter);
                Scan scan = new Scan();
                scan.setRowPrefixFilter(Bytes.toBytes(user1ID));

                final long startTime = System.currentTimeMillis();
                ResultScanner rs = table.getScanner(scan);
                Result r = rs.next();
                int numResults = 0;
                if (r == null) {
                    LOGGER.warn("result set is empty");
                } else {
                    for (; r != null; r = rs.next()) {
                        String text = Bytes.toString(r.getValue(Bytes.toBytes("tweet"),
                                Bytes.toBytes("text")));
                        // LOGGER.info(text);
                        numResults++;
                    }
                    final long endTime = System.currentTimeMillis();
                    LOGGER.info(String.format("%d results returned in %d ms",
                            numResults, endTime - startTime));
                }
            }
        }

        return 1;
    }
}
