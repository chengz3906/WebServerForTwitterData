package cmu.cc.team.spongebob.query3.database;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;

import java.sql.SQLException;
import java.util.List;

public class MySQLResultSetWrapper implements TweetResultSetWrapper {

    private final List<JsonObject> rows;
    private int index;
    private int len;

    public MySQLResultSetWrapper(ResultSet rs) {
        this.rows = rs.getRows();
        this.index = 0;
        this.len = this.rows.size();
    }

    @Override
    public Tweet next() {
        if (index >= len) {
            return null;
        }
        JsonObject row = rows.get(index);
        long tweetId = row.getLong("tweet_id");
        String text = row.getString("text");
        String censoredText = row.getString("censored_text");
        double impactScore = row.getLong("impact_score");
        index++;
        return new Tweet(text, censoredText, tweetId, impactScore);
    }

    @Override
    public void close() {
        return;
    }
}
