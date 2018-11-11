package cmu.cc.team.spongebob.query3.database;

import java.io.IOException;
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
    private static TableName tableName = TableName.valueOf("contact_tweet");
    /**
     * Logger.
     */
    private static final Logger LOGGER = Logger.getRootLogger();

    private static final byte[] family = Bytes.toBytes("tweet");
    private static final byte[] userId = Bytes.toBytes("user_id");
    private static final byte[] createdAt = Bytes.toBytes("created_at");

    /**
     * Configuration.
     */
    private static Configuration conf;

    public TopicWordHBaseBackend() {
        LOGGER.setLevel(Level.OFF);
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
    }

    public String query(int n1, int n2) {

        // Get contact information
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Table twitterTable = conn.getTable(tableName)) {
            Scan scan = new Scan();
//            BinaryPrefixComparator comp = new BinaryPrefixComparator(userIdBytes);
//            Filter filter = new RowFilter(
//                    CompareFilter.CompareOp.EQUAL, comp) {
//            };

//            PrefixFilter filter = new PrefixFilter(userIdBytes);
//            scan.setFilter(filter);
            ResultScanner rs = twitterTable.getScanner(scan);
            HBaseResultSetWrapper rsWrapper = new HBaseResultSetWrapper(rs);
            return TopicScoreCalculator.getTopicScore(rsWrapper, n1, n2);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
