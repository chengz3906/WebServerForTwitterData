package cmu.cc.team.spongebob.query3.database;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import cmu.cc.team.spongebob.query2.database.ContactUser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class TopicWordHBaseBackend {

    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = System.getenv("HBASE_DNS");
    /**
     * The name of your HBase table.
     */
    private static TableName tableName = TableName.valueOf(System.getenv("HBASE_TABLE_NAME"));
    /**
     * Logger.
     */
    private static final Logger LOGGER = Logger.getRootLogger();

    private static final byte[] family = Bytes.toBytes("tweet");
    private static final byte[] createdAt = Bytes.toBytes("created_at");

    /**
     * Configuration.
     */
    private static Configuration conf;

    private static final TopicScoreCalculator topicScoreCalculator = new TopicScoreCalculator();

    private static Connection conn;

    private static Table twitterTable;

    public TopicWordHBaseBackend() throws IOException {
        LOGGER.setLevel(Level.OFF);
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        conn = ConnectionFactory.createConnection(conf);
        twitterTable = conn.getTable(tableName);
    }

    public String query(long uidStart, long uidEnd,
                        long timeStart, long timeEnd, int n1, int n2) {

        ByteBuffer bf = ByteBuffer.allocate(16);
        bf.putLong(uidStart);
        bf.putLong(8, timeStart);
        byte[] startByte = bf.array();
        bf = ByteBuffer.allocate(16);
        bf.putLong(uidEnd);
        bf.putLong(8, timeEnd);
        byte[] endByte = bf.array();

        byte[] timeStartByte = Bytes.toBytes(timeStart);
        byte[] timeEndByte = Bytes.toBytes(timeEnd);
        // Get contact information
        try {
            Scan scan = new Scan();
            scan.withStartRow(startByte);
            scan.withStopRow(endByte);
            BinaryComparator timeStartComp = new BinaryComparator(timeStartByte);
            BinaryComparator timeEndComp = new BinaryComparator(timeEndByte);
            Filter timeStartFilter = new SingleColumnValueFilter(
                    family, createdAt, CompareFilter.CompareOp.GREATER_OR_EQUAL, timeStartComp
            );
            Filter timeEndFilter = new SingleColumnValueFilter(
                    family, createdAt, CompareFilter.CompareOp.LESS_OR_EQUAL, timeEndComp
            );
            FilterList filters = new FilterList();
            filters.addFilter(timeStartFilter);
            filters.addFilter(timeEndFilter);
            scan.setFilter(filters);
            ResultScanner rs = twitterTable.getScanner(scan);
            HBaseResultSetWrapper rsWrapper = new HBaseResultSetWrapper(rs);
            return topicScoreCalculator.getTopicScore(rsWrapper, n1, n2);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
