package cmu.cc.team.spongebob.query2.database;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class TweetIntimacyHBaseBackend {

    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = System.getenv("MYSQL_DNS");
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

    private static final byte[] user1Family = Bytes.toBytes("user1");
    private static final byte[] user2Family = Bytes.toBytes("user2");
    private static final byte[] tweetFamily = Bytes.toBytes("tweet");
    private static final byte[] idBytes = Bytes.toBytes("id");
    private static final byte[] screenNameBytes = Bytes.toBytes("screen_name");
    private static final byte[] descriptionBytes = Bytes.toBytes("description");
    private static final byte[] textBytes = Bytes.toBytes("text");
    private static final byte[] createdAtBytes = Bytes.toBytes("created_at");
    private static final byte[] intimacyScoreBytes = Bytes.toBytes("intimacy_score");

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
        HashMap<Long, Integer> contactsIndex = new HashMap<>();
        ArrayList<ContactUser> contacts = new ArrayList<>();

        // Get contact information
        try (Connection conn = ConnectionFactory.createConnection(conf);
             Table tweetIntimacyTable = conn.getTable(tableName)) {
            Scan scan = new Scan();
            byte[] userIdBytes = Bytes.toBytes(userId);
            BinaryComparator compId = new BinaryComparator(userIdBytes);
            BinaryPrefixComparator compKey = new BinaryPrefixComparator(userIdBytes);
            Filter filterId = new SingleColumnValueFilter(
                    user1Family, idBytes, CompareFilter.CompareOp.EQUAL, compId
            );
            Filter filterKey = new RowFilter(
                    CompareFilter.CompareOp.EQUAL, compKey
            );
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filterList.addFilter(filterId);
            filterList.addFilter(filterKey);
            scan.setFilter(filterList);
            ResultScanner rs = tweetIntimacyTable.getScanner(scan);
            for (Result r = rs.next(); r != null; r = rs.next()) {
                Long id = Bytes.toLong(r.getValue(user2Family, idBytes));
                String screenName = Bytes.toString(r.getValue(user2Family, screenNameBytes));
                String description = Bytes.toString(r.getValue(user2Family, descriptionBytes));
                String text = Bytes.toString(r.getValue(tweetFamily, textBytes));
                String createdAt = Bytes.toString(r.getValue(tweetFamily, createdAtBytes));
                double intimacyScore = Bytes.toDouble(r.getValue(tweetFamily, intimacyScoreBytes));
                if (!contactsIndex.containsKey(id)) {
                    contactsIndex.put(id, contacts.size());
                    contacts.add(new ContactUser(id, screenName,
                            description, intimacyScore));
                }
                contacts.get(contactsIndex.get(id)).addTweet(text, phrase, createdAt);
            }
            rs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Collections.sort(contacts);
        return contacts;
    }
}
