package cmu.cc.team.spongebob.etl.phase1.s3_to_hfile;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;

public class BulkLoadMapperTest {
    private MapDriver<LongWritable, Text, LongWritable, Text> driver;

    /*
    @Before
    public void setUp() {
        Mapper<LongWritable, Text, LongWritable, Text> mapper = new ContactTweetMapper();
        driver = new MapDriver<>(mapper);
    }

    @Test
    public void testWordCountMapper() throws IOException {
        driver.withInput(new LongWritable(), new Text("{\"user1_id\": 123}"))
                .withInput(new LongWritable(), new Text("{\"user1_id\": 999}"))
                .withOutput(new LongWritable(123), new Text("{\"user1_id\": 123}"))
                .withOutput(new LongWritable(999), new Text("{\"user1_id\": 999}"))
                .runTest(false);
    }
    */
}
