package cmu.cc.team.spongebob.etl.phase1;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.Cleanup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;

import java.io.IOException;


public class HBaseETLJob {
    public static class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        private static final String COLF_USER1 = "user1";
        private static final String COLF_USER2 = "user2";
        private static final String COLF_TWEET = "tweet";
        private static final String COL_USER_ID = "id";
        private static final String COL_USER_SCREEN_NAME = "screen_name";
        private static final String COL_USER_DESC = "description";
        private static final String COL_TWEET_TEXT = "text";
        private static final String COL_TWEET_CREATED_AT = "created_at";

        private final JsonParser jsonParser = new JsonParser();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            JsonObject rowJSON = jsonParser.parse(value.toString()).getAsJsonObject();

            String user1ID = rowJSON.get("user1_id").getAsString();
            String user1ScreenName = rowJSON.get("user1_screen_name").getAsString();
            String user1Description = rowJSON.get("user1_desc").getAsString();

            String user2ID = rowJSON.get("user2_id").getAsString();
            String user2ScreenName = rowJSON.get("user2_screen_name").getAsString();
            String user2Description = rowJSON.get("user2_des").getAsString();

            String tweetText = rowJSON.get("tweet_text").getAsString();
            String tweetCreatedAt = rowJSON.get("created_at").getAsString();

            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(user1ID));
            Put put = new Put(user1ID.getBytes());

            // user1
            put.addColumn(COLF_USER1.getBytes(), COL_USER_SCREEN_NAME.getBytes(), user1ScreenName.getBytes());
            put.addColumn(COLF_USER1.getBytes(), COL_USER_DESC.getBytes(), user1Description.getBytes());

            // user2
            put.addColumn(COLF_USER2.getBytes(), COL_USER_ID.getBytes(), user2ID.getBytes());
            put.addColumn(COLF_USER2.getBytes(), COL_USER_SCREEN_NAME.getBytes(), user2ScreenName.getBytes());
            put.addColumn(COLF_USER2.getBytes(), COL_USER_DESC.getBytes(), user2Description.getBytes());

            // tweet
            put.addColumn(COLF_TWEET.getBytes(), COL_TWEET_TEXT.getBytes(), tweetText.getBytes());
            put.addColumn(COLF_TWEET.getBytes(), COL_TWEET_CREATED_AT.getBytes(), tweetCreatedAt.getBytes());

            context.write(rowKey, put);
        }
    }

    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf,"Phase1_HBase_ETL");
        job.setJarByClass(HBaseETLJob.class);

        // set s3 credentials
        job.getConfiguration().set("fs.s3n.awsAccessKeyId", System.getenv("S3_ACCESS_KEY"));
        job.getConfiguration().set("fs.s3n.awsSecretAccessKey", System.getenv("S3_SECRET_KEY"));

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setSpeculativeExecution(false);
        job.setReduceSpeculativeExecution(false);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);

        job.setMapperClass(HBaseETLJob.BulkLoadMapper.class);

        FileInputFormat.setInputPaths(job, inputPath);
        HFileOutputFormat2.setOutputPath(job, new Path(outputPath));

        @Cleanup Connection hbCon = ConnectionFactory.createConnection(conf);
        @Cleanup Table hTable = hbCon.getTable(TableName.valueOf("tweet_intimacy"));
        @Cleanup RegionLocator regionLocator = hbCon.getRegionLocator(TableName.valueOf("tweet_intimacy"));
        HFileOutputFormat2.configureIncrementalLoad(job, hTable, regionLocator);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}