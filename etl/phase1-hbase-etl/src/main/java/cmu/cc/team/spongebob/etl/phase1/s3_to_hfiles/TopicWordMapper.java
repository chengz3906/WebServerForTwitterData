package cmu.cc.team.spongebob.etl.phase1.s3_to_hfiles;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;


public class TopicWordMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    private static final JsonParser jsonParser = new JsonParser();
    private static final byte[] COLF_TWEET = "tweet".getBytes();
    private static final byte[] COL_TWEET_ID = "id".getBytes();
    private static final byte[] COL_TWEET_TEXT = "text".getBytes();
    private static final byte[] COL_TWEET_CENSORED_TEXT = "censored_text".getBytes();
    private static final byte[] COL_IMPACT_SCORE = "impact_score".getBytes();
    private static final byte[] COL_TWEET_CREATED_AT = "created_at".getBytes();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        JsonObject rowJSON = jsonParser.parse(value.toString()).getAsJsonObject();

        // row key
        long userID = rowJSON.get("user_id").getAsLong();
        long tweetID = rowJSON.get("tweet_id").getAsLong();
        ByteBuffer byteBuffer = ByteBuffer.allocate(16); // 8 byte for user_id, 8 byte for _id
        byteBuffer.putLong(userID);
        byteBuffer.putLong(8, tweetID);
        byte[] rowKeyBytes = byteBuffer.array();
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowKeyBytes);
        Put put = new Put(rowKeyBytes);

        // impact score
        double impactScore = rowJSON.get("impact_score").getAsDouble();
        put.addColumn(COLF_TWEET, COL_IMPACT_SCORE, Bytes.toBytes(impactScore));

        // text
        String text = rowJSON.get("text").getAsString();
        put.addColumn(COLF_TWEET, COL_TWEET_TEXT, Bytes.toBytes(text));

        // censored text
        String censoredText = rowJSON.get("censored_text").getAsString();
        put.addColumn(COLF_TWEET, COL_TWEET_CENSORED_TEXT, Bytes.toBytes(censoredText));

        // tweet created_at
        long tweetCreatedAt = rowJSON.get("created_at").getAsLong();
        put.addColumn(COLF_TWEET, COL_TWEET_CREATED_AT, Bytes.toBytes(tweetCreatedAt));

        // teweet id
        put.addColumn(COLF_TWEET, COL_TWEET_ID, Bytes.toBytes(tweetID));

        context.write(rowKey, put);
    }
}