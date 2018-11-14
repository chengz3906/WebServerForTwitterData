package cmu.cc.team.spongebob.etl.phase1.s3_to_hfiles;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;


public class LoadHBase extends Configured implements Tool {
    private static final Log LOGGER = LogFactory.getLog(LoadHBase.class);

    private static final JsonParser jsonParser = new JsonParser();
    private static final byte[] COLF_USER2 = "user2".getBytes();
    private static final byte[] COLF_TWEET = "tweet".getBytes();
    private static final byte[] COL_USER_ID = "id".getBytes();
    private static final byte[] COL_USER_SCREEN_NAME = "screen_name".getBytes();
    private static final byte[] COL_USER_DESC = "description".getBytes();
    private static final byte[] COL_USER_INTIMACY = "intimacy".getBytes();
    private static final byte[] COL_TWEET_TEXT = "text".getBytes();
    private static final byte[] COL_TWEET_CREATED_AT = "created_at".getBytes();

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(HBaseConfiguration.create(), new LoadHBase(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(getConf());
             Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(args[2]);

            createTable(admin, tableName);
            Table table = connection.getTable(tableName);

            LOGGER.info("start loading HBase...");

            try(BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
                for(String line; (line = br.readLine()) != null; ) {
                    JsonObject jsonObject = jsonParser.parse(line).getAsJsonObject();
                    putOneJSON(jsonObject, table);
                }
            }
        }

        LOGGER.info("HBase ETL Job started...");
        return 1;
    }

    private static void putOneJSON(JsonObject rowJSON, Table table) throws IOException {
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

        table.put(put);
    }

    private static void createTable(Admin admin, TableName tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            LOGGER.warn("htable already exists, deleting htable...");
            try {
                admin.disableTable(tableName);
            } catch (TableNotEnabledException e) {
                // do nothing
            }
            admin.deleteTable(tableName);
        }

        HTableDescriptor htd = new HTableDescriptor(tableName);
        htd.addFamily(new HColumnDescriptor(Bytes.toBytes("user2")));
        htd.addFamily(new HColumnDescriptor(Bytes.toBytes("tweet")));
        LOGGER.info("creating htable...");
        admin.createTable(htd);
    }
}
