package cmu.cc.team.spongebob.etl.phase1.s3_to_hfile;

import cmu.cc.team.spongebob.etl.phase1.s3_to_hfiles.BulkLoadReducer;
import com.google.gson.JsonObject;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

public class BulkLoadReducerTest {
    private ReduceDriver<LongWritable, Text, ImmutableBytesWritable, KeyValue> driver;

    @Before
    public void setUp() {
        Reducer<LongWritable, Text, ImmutableBytesWritable, Put> reducer = new BulkLoadReducer();
        driver = new ReduceDriver<>(reducer);
        driver.setOutputSerializationConfiguration(HFileOutputFormat2.);
    }

    @Test
    public void testWordCountMapper() throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(16);
        byteBuffer.putLong(123L);
        byteBuffer.putLong(8, 3L);

        JsonObject rowJsonObj = new JsonObject();
        rowJsonObj.addProperty("user1_id", 123L);
        rowJsonObj.addProperty("user1_screen_name", "allen");
        rowJsonObj.addProperty("user1_desc", "i like cc");
        rowJsonObj.addProperty("user2_id", 56L);
        rowJsonObj.addProperty("user2_screen_name", "bob");
        rowJsonObj.addProperty("user2_desc", "i like ml");
        rowJsonObj.addProperty("tweet_text", "thank you, bob");
        rowJsonObj.addProperty("intimacy_score", 0.69);
        rowJsonObj.addProperty("created_at", "2014-04-08T05:33:29.000Z");
        Text rowJsonStr = new Text(rowJsonObj.toString());

        byte[] rowKey = byteBuffer.array();
        Put put = new Put(rowKey);

        // user1
        put.addColumn("user1".getBytes(), "screen_name".getBytes(), "allen".getBytes());
        put.addColumn("user1".getBytes(), "description".getBytes(), "i like cc".getBytes());

        // user2
        put.addColumn("user2".getBytes(), "id".getBytes(), Bytes.toBytes(56L));
        put.addColumn("user2".getBytes(), "screen_name".getBytes(), "bob".getBytes());
        put.addColumn("user2".getBytes(), "description".getBytes(), "i like ml".getBytes());
        put.addColumn("user2".getBytes(), "intimacy".getBytes(), Bytes.toBytes(0.69));

        // tweet
        put.addColumn("tweet".getBytes(), "text".getBytes(), "thank you, bob".getBytes());
        put.addColumn("tweet".getBytes(), "created_at".getBytes(), "2014-04-08T05:33:29.000Z".getBytes());

        driver.withInput(new LongWritable(123), Collections.singletonList(rowJsonStr))
                .withOutput(new ImmutableBytesWritable(rowKey), put)
                .runTest(false);
    }
}

