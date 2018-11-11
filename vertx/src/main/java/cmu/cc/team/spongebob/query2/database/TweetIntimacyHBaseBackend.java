package cmu.cc.team.spongebob.query2.database;

import java.io.IOException;
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
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class TweetIntimacyHBaseBackend {

    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = "localhost";
    /**
     * The name of your HBase table.
     */
    private static TableName tableName = TableName.valueOf("contact_tweet");
    /**
     * Logger.
     */
    private static final Logger LOGGER = Logger.getRootLogger();

    private static final byte[] userFamily = Bytes.toBytes("user2");
    private static final byte[] tweetFamily = Bytes.toBytes("tweet");
    private static final byte[] idBytes = Bytes.toBytes("id");
    private static final byte[] screenNameBytes = Bytes.toBytes("screen_name");
    private static final byte[] descriptionBytes = Bytes.toBytes("description");
    private static final byte[] textBytes = Bytes.toBytes("text");
    private static final byte[] createdAtBytes = Bytes.toBytes("created_at");
    private static final byte[] intimacyScoreBytes = Bytes.toBytes("intimacy");

    /**
     * Configuration.
     */
    private static Configuration conf;
    private final Connection conn;
    private final Table twitterTable;

    public TweetIntimacyHBaseBackend() throws IOException {
        LOGGER.setLevel(Level.OFF);
//        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
//            System.out.print("Malformed HBase IP address");
//            System.exit(-1);
//        }
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");

        conn = ConnectionFactory.createConnection(conf);
        twitterTable = conn.getTable(tableName);
    }


    public ArrayList<ContactUser> query(Long userId, String phrase) {
        ArrayList<ContactUser> contacts = new ArrayList<>();

        // Get contact information
        Scan scan = new Scan();
        byte[] userIdBytes = Bytes.toBytes(userId);
//            BinaryPrefixComparator comp = new BinaryPrefixComparator(userIdBytes);
//            Filter filter = new RowFilter(
//                    CompareFilter.CompareOp.EQUAL, comp) {
//            };

//            PrefixFilter filter = new PrefixFilter(userIdBytes);
//            scan.setFilter(filter);
        scan.setRowPrefixFilter(userIdBytes);
        try {
            ResultScanner rs = twitterTable.getScanner(scan);
            Long lastUid = null;
            for (Result r = rs.next(); r != null; r = rs.next()) {
                Long id = Bytes.toLong(r.getValue(userFamily, idBytes));
                String screenName = Bytes.toString(r.getValue(userFamily, screenNameBytes));
                String description = Bytes.toString(r.getValue(userFamily, descriptionBytes));
                String text = Bytes.toString(r.getValue(tweetFamily, textBytes));
                String createdAt = Bytes.toString(r.getValue(tweetFamily, createdAtBytes));
                double intimacyScore = Bytes.toDouble(r.getValue(userFamily, intimacyScoreBytes));
//                System.out.println(screenName+" "+description+" "+text+" "+createdAt+" "+intimacyScore);
                if (!id.equals(lastUid)) {
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
