package cmu.cc.team.spongebob.etl.phase1.s3_to_hfiles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;


public class TweetGet {

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();

        long user1ID = Long.parseLong(args[0]);
        ByteBuffer rowkeyBuffer = ByteBuffer.allocate(16);
        rowkeyBuffer.putLong(user1ID);
        rowkeyBuffer.putLong(8, 0);
        byte[] rowkey = rowkeyBuffer.array();

        TableName tableName = TableName.valueOf(args[1]);

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            try (Table table = connection.getTable(tableName)) {
                for (int i=0; i < 100; i++) {
                    Get get = new Get(rowkey);

                    final long startTime = System.currentTimeMillis();
                    Result rs = table.get(get);
                    if (rs == null) {
                        final long endTime = System.currentTimeMillis();
                        System.out.println(String.format("0 result found in %d ms",
                                endTime - startTime));
                    } else {

                        String text = Bytes.toString(rs.getValue(Bytes.toBytes("tweet"),
                                Bytes.toBytes("text")));

                        final long endTime = System.currentTimeMillis();
                        System.out.println(String.format("1 result found in %d ms", endTime - startTime));
                        System.out.println(text);
                    }
                }
            }
        }
    }
}
