package cmu.cc.team.spongebob.etl.phase1.s3_to_hfiles;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class TopicWordMapper {

    private static final JsonParser jsonParser = new JsonParser();
    private static final byte[] COLF_TWEET = "tweet".getBytes();
    private static final byte[] COL_TWEET_ID = "id".getBytes();
    private static final byte[] COL_TWEET_TEXT = "text".getBytes();
    private static final byte[] COL_TWEET_CENSORED_TEXT = "censored_text".getBytes();
    private static final byte[] COL_IMPACT_SCORE = "impact_score".getBytes();
    private static final byte[] COL_TWEET_CREATED_AT = "created_at".getBytes();
    private static String S3_FOLDER_NAME = "topic_word_json";
    private static String S3_BUCKET_NAME = "cmucc-team-phase2";

    public static void main(String[] args) throws IOException {
        String clientRegion = "us-east-1";
        String bucketName = S3_BUCKET_NAME;
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(clientRegion)
                .build();
        List<String> keyList = getKeyList(s3Client, bucketName);

        Configuration config = HBaseConfiguration.create();
        try (
                Connection connection = ConnectionFactory.createConnection(config);
                Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(args[0]);

            createTable(admin, tableName);
            Table table = connection.getTable(tableName);

            System.out.println("start loading HBase...");

            for (String key : keyList) {
                DownloadObject(s3Client, bucketName, key);
                loadOneJSON(key, table, Integer.parseInt(args[1]));
                DeleteLocalFile(key);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("HBase ETL Job finished...");
    }

    private static List<String> getKeyList(AmazonS3 s3Client, String bucketName) {
        List<String> keyList = new ArrayList<>();
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName)
                .withPrefix(S3_FOLDER_NAME);
        ListObjectsV2Result result;
        do {
            result = s3Client.listObjectsV2(req);

            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                String key = objectSummary.getKey();
                if (key.endsWith(".json")) {
                    keyList.add(key);
                }
            }
            // If there are more than maxKeys keys in the bucket, get a continuation token
            // and list the next objects.
            String token = result.getNextContinuationToken();
            req.setContinuationToken(token);
        } while (result.isTruncated());

        return keyList;
    }

    private static void loadOneJSON(String filename, Table table, int batchSize) {
        System.out.println("Write to HBase...");
        final long startTime = System.currentTimeMillis();
        List<Put> putList = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            for (String line; (line = br.readLine()) != null; ) {
                JsonObject jsonObject = jsonParser.parse(line).getAsJsonObject();
                putList.add(putOneJSON(jsonObject));
                if (putList.size() == batchSize) {
                    table.put(putList);
                    putList.clear();
                }
            }
            if (putList.size() != 0) {
                table.put(putList);
                putList.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        final long endTime = System.currentTimeMillis();
        System.out.println(String.format("Load a json in %d s", (endTime - startTime) / 1000));
    }

    private static Put putOneJSON(JsonObject rowJSON) {
        // row key
        long userID = rowJSON.get("user_id").getAsLong();
//        long tweetID = rowJSON.get("tweet_id").getAsLong();
        long tweetCreateAt = rowJSON.get("created_at").getAsLong();
        ByteBuffer byteBuffer = ByteBuffer.allocate(16); // 8 byte for user_id, 8 byte for _id
        byteBuffer.putLong(userID);
//        byteBuffer.putLong(8, tweetID);
        byteBuffer.putLong(8, tweetCreateAt);
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

        // teweet id
        long tweetID = rowJSON.get("tweet_id").getAsLong();
        put.addColumn(COLF_TWEET, COL_TWEET_ID, Bytes.toBytes(tweetID));

        // tweet created_at
        put.addColumn(COLF_TWEET, COL_TWEET_CREATED_AT, Bytes.toBytes(tweetCreateAt));


        return put;
    }

    private static void createTable(Admin admin, TableName tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            System.out.println("htable already exists, deleting htable...");
            try {
                admin.disableTable(tableName);
            } catch (TableNotEnabledException e) {
                // do nothing
            }
            admin.deleteTable(tableName);
        }

        HTableDescriptor htd = new HTableDescriptor(tableName);
        htd.addFamily(new HColumnDescriptor(Bytes.toBytes("tweet")));
        System.out.println("creating htable...");
        admin.createTable(htd);
    }

    private static void DownloadObject(AmazonS3 s3Client, String bucketName, String key) {
        System.out.println("Downloading " + key + "...");
        File localFile = new File(key);
        ObjectMetadata metadata = s3Client.getObject(new GetObjectRequest(bucketName, key),
                localFile);
        System.out.println("Finished download.");
    }

    private static void DeleteLocalFile(String filename) {
        File file = new File(filename);
        if (file.delete()) {
            System.out.println("Delete file.");
        } else {
            System.out.println("Fail to delete file.");
        }
    }
}