package cmu.cc.team.spongebob.etl.phase1.s3_to_hfiles;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.nio.ByteBuffer;


public class TweetGet extends Configured implements Tool {
    private static final Log LOGGER = LogFactory.getLog(HBaseETLJob.class);

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(HBaseConfiguration.create(), new TweetScan(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        long user1ID = Long.parseLong(args[0]);
        ByteBuffer rowkeyBuffer = ByteBuffer.allocate(16);
        rowkeyBuffer.putLong(user1ID);
        rowkeyBuffer.putLong(8, 0);
        byte[] rowkey = rowkeyBuffer.array();

        TableName tableName = TableName.valueOf(args[1]);

        try (Connection connection = ConnectionFactory.createConnection(getConf())) {
            try (Table table = connection.getTable(tableName)) {
                Get get = new Get(rowkey);

                final long startTime = System.currentTimeMillis();
                Result rs = table.get(get);
                if (rs == null) {
                    final long endTime = System.currentTimeMillis();
                    LOGGER.info(String.format("0 result found in %d ms", endTime - startTime));
                } else {

                    String text = Bytes.toString(rs.getValue(Bytes.toBytes("tweet"),
                            Bytes.toBytes("text")));

                    final long endTime = System.currentTimeMillis();
                    LOGGER.info(String.format("1 result found in %d ms", endTime - startTime));
                    LOGGER.info(text);
                }
            }
        }

        return 1;
    }
}
