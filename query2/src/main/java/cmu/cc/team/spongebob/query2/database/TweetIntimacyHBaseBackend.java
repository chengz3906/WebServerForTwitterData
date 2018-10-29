package cmu.cc.team.spongebob.query2.database;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class TweetIntimacyHBaseBackend {

    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = "172.31.1.50";
    /**
     * The name of your HBase table.
     */
    private static TableName tableName = TableName.valueOf("twitter");
    /**
     * Logger.
     */
    private static final Logger LOGGER = Logger.getRootLogger();

    /**
     * Configuration.
     */
    private static Configuration conf;

    public TweetIntimacyHBaseBackend() {
        LOGGER.setLevel(Level.OFF);
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("Malformed HBase IP address");
            System.exit(-1);
        }
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
    }

    public ArrayList<ContactUser> query(Long userId, String phrase) {
        ArrayList<ContactUser> contacts = new ArrayList<>();
        byte[] userFamily = Bytes.toBytes("user2");
        byte[] tweetFamily = Bytes.toBytes("tweet");
        byte[] idBytes = Bytes.toBytes("id");
        byte[] screenNameBytes = Bytes.toBytes("screen_name");
        byte[] descriptionBytes = Bytes.toBytes("description");
        byte[] textBytes = Bytes.toBytes("text");
        byte[] createdAtBytes = Bytes.toBytes("created_at");
        byte[] intimacyScoreBytes = Bytes.toBytes("intimacy_score");

        // Get contact information
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Table bizTable = conn.getTable(tableName)) {
            Scan scan = new Scan();
            byte[] userIdBytes = Bytes.toBytes(userId);
            BinaryComparator comp = new BinaryComparator(userIdBytes);
            Filter filter = new RowFilter(
                    CompareFilter.CompareOp.EQUAL, comp);
            scan.setFilter(filter);
            ResultScanner rs = bizTable.getScanner(scan);
            Long lastUid = null;
            for (Result r = rs.next(); r != null; r = rs.next()) {
                Long id = Bytes.toLong(r.getValue(userFamily, idBytes));
                String screenName = Bytes.toString(r.getValue(userFamily, screenNameBytes));
                String description = Bytes.toString(r.getValue(userFamily, descriptionBytes));
                String text = Bytes.toString(r.getValue(tweetFamily, textBytes));
                String createdAt = Bytes.toString(r.getValue(tweetFamily, createdAtBytes));
                double intimacyScore = Bytes.toDouble(r.getValue(tweetFamily, intimacyScoreBytes));
                if (id.equals(lastUid)) {
                    contacts.add(new ContactUser(id, screenName,
                            description, intimacyScore));
                    lastUid = id;
                }
                contacts.get(contacts.size() - 1).addTweet(text, phrase, createdAt);
            }
            rs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Collections.sort(contacts);
        return contacts;
    }
}
