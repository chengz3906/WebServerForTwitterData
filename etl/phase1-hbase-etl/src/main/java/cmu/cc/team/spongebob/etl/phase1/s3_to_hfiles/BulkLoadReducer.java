package cmu.cc.team.spongebob.etl.phase1.s3_to_hfiles;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.ByteBuffer;


public class BulkLoadReducer extends Reducer<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
    private static final byte[] COLF_USER1 = "user1".getBytes();
    private static final byte[] COLF_USER2 = "user2".getBytes();
    private static final byte[] COLF_TWEET = "tweet".getBytes();
    private static final byte[] COL_USER_ID = "id".getBytes();
    private static final byte[] COL_USER_SCREEN_NAME = "screen_name".getBytes();
    private static final byte[] COL_USER_DESC = "description".getBytes();
    private static final byte[] COL_USER_INTIMACY = "intimacy".getBytes();
    private static final byte[] COL_TWEET_TEXT = "text".getBytes();
    private static final byte[] COL_TWEET_CREATED_AT = "created_at".getBytes();

    private static final JsonParser jsonParser = new JsonParser();

    public void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        long counter = 0L;
        for (Text value: values) {
            writeOneRow(value.toString(), counter++, context);
        }
    }

    private void writeOneRow(String row, Long id, Context context)
            throws IOException, InterruptedException {
        JsonObject rowJSON = jsonParser.parse(row).getAsJsonObject();

        long user1ID = rowJSON.get("user1_id").getAsLong();
        String user1ScreenName = rowJSON.get("user1_screen_name").getAsString();
        String user1Description = rowJSON.get("user1_desc").getAsString();

        long user2ID = rowJSON.get("user2_id").getAsLong();
        String user2ScreenName = rowJSON.get("user2_screen_name").getAsString();
        String user2Description = rowJSON.get("user2_desc").getAsString();

        double intimacyScore = rowJSON.get("intimacy_score").getAsDouble();

        String tweetText = rowJSON.get("tweet_text").getAsString();
        String tweetCreatedAt = rowJSON.get("created_at").getAsString();

        ByteBuffer byteBuffer = ByteBuffer.allocate(16); // 8 byte for user_id, 8 byte for _id
        byteBuffer.putLong(user1ID);
        byteBuffer.putLong(8, id);
        byte[] rowKeyBytes = byteBuffer.array();
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowKeyBytes);
        Put put = new Put(rowKeyBytes);

        // user1
        if (user1ScreenName != null) {
            KeyValue user1ScreenNameKV = new KeyValue(rowKeyBytes, COLF_USER1, COL_USER_SCREEN_NAME, user1ScreenName.getBytes());
            context.write(rowKey, user1ScreenNameKV);
        }

        if (user1Description != null) {
            KeyValue user1DescKV = new KeyValue(rowKeyBytes, COLF_USER1, COL_USER_DESC, user1Description.getBytes());
            context.write(rowKey, user1DescKV);
        }

        // user2
        KeyValue user2IDKeyValue = new KeyValue(rowKeyBytes, COLF_USER2, COL_USER_ID, Bytes.toBytes(user2ID));
        context.write(rowKey, user2IDKeyValue);

        KeyValue user2KV = new KeyValue(rowKeyBytes, COLF_USER2, COL_USER_INTIMACY, Bytes.toBytes(intimacyScore));
        context.write(rowKey, user2KV);
        if (user2ScreenName != null) {
            KeyValue user2ScreenNameKV = new KeyValue(rowKeyBytes, COLF_USER2, COL_USER_SCREEN_NAME, user2ScreenName.getBytes());
            context.write(rowKey, user2ScreenNameKV);
        }
        if (user2Description != null) {
            KeyValue user2Desc = new KeyValue(rowKeyBytes, COLF_USER2, COL_USER_DESC, user2Description.getBytes());
            context.write(rowKey, user2Desc);
        }

        // tweet
        if (tweetText != null) {
            KeyValue tweetTextKV = new KeyValue(rowKeyBytes, COLF_TWEET, COL_TWEET_TEXT, tweetText.getBytes());
            context.write(rowKey, tweetTextKV);
        }
        KeyValue tweetCreatedAtKV = new KeyValue(rowKeyBytes, COLF_TWEET, COL_TWEET_CREATED_AT, tweetCreatedAt.getBytes());
        context.write(rowKey, tweetCreatedAtKV);
    }
}
