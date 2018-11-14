package cmu.cc.team.spongebob.etl.hbase;

import com.google.gson.JsonElement;
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

public class ContactTweetLoader {
    private static final byte[] COLF_USER2 = "user2".getBytes();
    private static final byte[] COLF_TWEET = "tweet".getBytes();
    private static final byte[] COL_USER_ID = "id".getBytes();
    private static final byte[] COL_USER_SCREEN_NAME = "screen_name".getBytes();
    private static final byte[] COL_USER_DESC = "description".getBytes();
    private static final byte[] COL_USER_INTIMACY = "intimacy".getBytes();
    private static final byte[] COL_TWEET_TEXT = "text".getBytes();
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
//        htd.addFamily(new HColumnDescriptor(Bytes.toBytes("user2")));
//        htd.addFamily(new HColumnDescriptor(Bytes.toBytes("tweet")));
        htd.addFamily(new HColumnDescriptor(COLF_USER2));
        htd.addFamily(new HColumnDescriptor(COLF_TWEET));
        System.out.println(String.format("creating htable %s...", tableName));
        admin.createTable(htd);
    }

    public static Put putOneJSON(JsonObject rowJSON) {
        // row key
        long user1ID = rowJSON.get("user1_id").getAsLong();
        long rowKeyID = rowJSON.get("row_key_id").getAsLong();
        ByteBuffer byteBuffer = ByteBuffer.allocate(16); // 8 byte for user_id, 8 byte for _id
        byteBuffer.putLong(user1ID);
        byteBuffer.putLong(8, rowKeyID);
        byte[] rowKeyBytes = byteBuffer.array();
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowKeyBytes);
        Put put = new Put(rowKeyBytes);

        // user 2 id
        long user2ID = rowJSON.get("user2_id").getAsLong();
        put.addColumn(COLF_USER2, COL_USER_ID, Bytes.toBytes(user2ID));

        // intimacy score
        double intimacyScore = rowJSON.get("intimacy_score").getAsDouble();
        put.addColumn(COLF_USER2, COL_USER_INTIMACY, Bytes.toBytes(intimacyScore));

        // user 2 screen name
        JsonElement user2ScreenName = rowJSON.get("user2_screen_name");
        if (user2ScreenName != null) {
            put.addColumn(COLF_USER2, COL_USER_SCREEN_NAME, user2ScreenName.getAsString().getBytes());
        }

        // user 2 description
        JsonElement user2Description = rowJSON.get("user2_desc");
        if (user2Description != null) {
            put.addColumn(COLF_USER2, COL_USER_DESC, user2Description.getAsString().getBytes());
        }

        // tweet text
        JsonElement tweetText = rowJSON.get("tweet_text");
        if (tweetText != null) {
            put.addColumn(COLF_TWEET, COL_TWEET_TEXT, tweetText.getAsString().getBytes());
        }

        // tweet created_at
        String tweetCreatedAt = rowJSON.get("created_at").getAsString();
        put.addColumn(COLF_TWEET, COL_TWEET_CREATED_AT, tweetCreatedAt.getBytes());

//        table.put(put);
        return put;
    }
}
