package cmu.cc.team.spongebob.etl.phase1.s3_to_hfiles;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BulkLoadMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static final JsonParser jsonParser = new JsonParser();
    private static final LongWritable user1IDWriteable = new LongWritable();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // extract user1 ID
        JsonObject rowJSON = jsonParser.parse(value.toString()).getAsJsonObject();
        long user1ID = rowJSON.get("user1_id").getAsLong();
        user1IDWriteable.set(user1ID);

        context.write(user1IDWriteable, value);
    }
}