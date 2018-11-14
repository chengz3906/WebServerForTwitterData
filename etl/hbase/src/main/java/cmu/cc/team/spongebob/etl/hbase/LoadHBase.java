package cmu.cc.team.spongebob.etl.hbase;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class LoadHBase {
    private static final JsonParser jsonParser = new JsonParser();
    private static final String clientRegion = "us-east-1";
    private static final String bucketName = "cmucc-team-phase2";
    private static final String[] prefix = {"contact_tweet_json_hbase/", "topic_word_json/"};
    private static final String[] tableNameStr = {"contact_tweet", "topic_word"};
    private static final int batchSize = 1000;

    public static void main(String[] args) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(clientRegion)
                .build();

        for (int i = 0; i < 2; i++) {
            List<String> keyList = getKeyList(s3Client, bucketName, prefix[i]);

            Configuration config = HBaseConfiguration.create();
            try (Connection connection = ConnectionFactory.createConnection(config);
                 Admin admin = connection.getAdmin()) {
                TableName tableName = TableName.valueOf(tableNameStr[i]);
                if (i == 0) {
                    ContactTweetLoader.createTable(admin, tableName);
                } else {
                    TopicWordLoader.createTable(admin, tableName);
                }
                Table table = connection.getTable(tableName);

                System.out.println("start loading HBase...");

                for (String key : keyList) {
                    DownloadObject(s3Client, bucketName, key);
                    loadOneJSON(key, table, batchSize, i);
                    DeleteLocalFile(key);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("HBase ETL Job finished...");
    }

    private static List<String> getKeyList(AmazonS3 s3Client, String bucketName, String prefix) {
        List<String> keyList = new ArrayList<>();
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName)
                .withPrefix("contact_tweet_json_hbase");
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

    private static void loadOneJSON(String filename, Table table, int batchSize, int phase) {
        System.out.println("Write to HBase...");
        final long startTime = System.currentTimeMillis();
        List<Put> putList = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            for (String line; (line = br.readLine()) != null; ) {
                JsonObject jsonObject = jsonParser.parse(line).getAsJsonObject();
                if (phase == 0) {
                    putList.add(ContactTweetLoader.putOneJSON(jsonObject));
                } else {
                    putList.add(TopicWordLoader.putOneJSON(jsonObject));
                }
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

    private static void DownloadObject(AmazonS3 s3Client, String bucketName, String key) {
        System.out.println("Downloading " + key + "...");
        File localFile = new File(key);
        s3Client.getObject(new GetObjectRequest(bucketName, key), localFile);
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
