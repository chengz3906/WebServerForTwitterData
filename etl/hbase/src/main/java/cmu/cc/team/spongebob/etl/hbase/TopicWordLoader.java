package cmu.cc.team.spongebob.etl.hbase;

import com.google.gson.JsonObject;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TopicWordLoader {
    private static final byte[] COLF_TWEET = "tweet".getBytes();
    private static final byte[] COL_TWEET_ID = "id".getBytes();
    private static final byte[] COL_TWEET_TEXT = "text".getBytes();
    private static final byte[] COL_TWEET_CENSORED_TEXT = "censored_text".getBytes();
    private static final byte[] COL_IMPACT_SCORE = "impact_score".getBytes();
    private static final byte[] COL_TWEET_CREATED_AT = "created_at".getBytes();

    public static void createTable(Admin admin, TableName tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            System.out.println(String.format("htable %s already exists, deleting htable...",
                    tableName));
            try {
                admin.disableTable(tableName);
            } catch (TableNotEnabledException e) {
                // do nothing
                e.printStackTrace();
            }
            admin.deleteTable(tableName);
        }

        HTableDescriptor htd = new HTableDescriptor(tableName);
//        htd.addFamily(new HColumnDescriptor(Bytes.toBytes("tweet")));
        htd.addFamily(new HColumnDescriptor(COLF_TWEET));
        System.out.println(String.format("creating htable %s...", tableName));
        admin.createTable(htd);
    }

    public static Put putOneJSON(JsonObject rowJSON) {
        // row key
        long userID = rowJSON.get("user_id").getAsLong();
        long tweetCreateAt = rowJSON.get("created_at").getAsLong();
        ByteBuffer byteBuffer = ByteBuffer.allocate(16); // 8 byte for user_id, 8 byte for _id
        byteBuffer.putLong(userID);
        byteBuffer.putLong(8, tweetCreateAt);
        byte[] rowKeyBytes = byteBuffer.array();
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowKeyBytes);
        Put put = new Put(rowKeyBytes);

        // impact score
        long impactScore = rowJSON.get("impact_score").getAsLong();
        put.addColumn(COLF_TWEET, COL_IMPACT_SCORE, Bytes.toBytes(impactScore));

        // text
        String text = rowJSON.get("text").getAsString();
        put.addColumn(COLF_TWEET, COL_TWEET_TEXT, Bytes.toBytes(text));

        // censored text
        String censoredText = rowJSON.get("censored_text").getAsString();
        put.addColumn(COLF_TWEET, COL_TWEET_CENSORED_TEXT, Bytes.toBytes(censoredText));

        // teweet id
        long tweetID = rowJSON.get("tweet_id").getAsLong();
        put.addColumn(COLF_TWEET, COL_TWEET_ID, Bytes.toBytes(tweetID));

        // tweet created_at
        put.addColumn(COLF_TWEET, COL_TWEET_CREATED_AT, Bytes.toBytes(tweetCreateAt));

        return put;
    }
}
