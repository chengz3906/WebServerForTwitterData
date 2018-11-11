package cmu.cc.team.spongebob.query3.database;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseResultSetWrapper implements TweetResultSetWrapper {

    private final ResultScanner rs;
    private final byte[] family = Bytes.toBytes("tweet");
    private final byte[] tweetIdBytes = Bytes.toBytes("id");
    private final byte[] textBytes = Bytes.toBytes("text");
    private final byte[] censoredTextBytes = Bytes.toBytes("censored_text");
    private final byte[] impactScoreBytes = Bytes.toBytes("impact_score");

    public HBaseResultSetWrapper(ResultScanner rs) {
        this.rs = rs;
    }

    @Override
    public Tweet next() {
        try {
            Result r = rs.next();
            if (r == null) return null;
            long tweetId = Bytes.toLong(r.getValue(family, tweetIdBytes));
            String text = Bytes.toString(r.getValue(family, textBytes));
            String censoredText = Bytes.toString(r.getValue(family, censoredTextBytes));
            double impactScore = Bytes.toDouble(r.getValue(family, impactScoreBytes));
            return new Tweet(text, censoredText, tweetId, impactScore);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {
        rs.close();
    }
}
