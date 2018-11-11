package cmu.cc.team.spongebob.query3.database;

import java.sql.ResultSet;
import java.sql.SQLException;

public class MySQLResultSetWrapper implements TweetResultSetWrapper {

    private final ResultSet rs;

    MySQLResultSetWrapper(ResultSet rs) {
        this.rs = rs;
    }

    @Override
    public Tweet next() {
        try {
            if (!rs.next()) return null;
            long tweetId = rs.getLong("tweet_id");
            String text = rs.getString("text");
            String censoredText = rs.getString("censored_text");
            double impactScore = rs.getLong("impact_score");
            return new Tweet(text, censoredText, tweetId, impactScore);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {
        try {
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
