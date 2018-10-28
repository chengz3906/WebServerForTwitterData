package cmu.cc.team.spongebob.query2.database;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.commons.dbcp2.BasicDataSource;

public class TweetIntimacyMySQLBackend {
    /**
     * JDBC driver of MySQL Connector/J.
     */
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    /**
     * Database name.
     */
    private static final String DB_NAME = System.getenv("MYSQL_DB_NAME");
    /**
     * DNS of Mysql database
     */
    private static final String DNS = System.getenv("MYSQL_DNS");
    /**
     * Database url
     */
    private static final String URL = String.format(
            "jdbc:mysql://%s/%s?useSSL=false", DNS, DB_NAME);
    /**
     * Username and password.
     */
    private static final String DB_USER = System.getenv("MYSQL_USER");
    private static final String DB_PWD = System.getenv("MYSQL_PWD");
    /**
     * The connection pool with the database.
     */
    private static BasicDataSource ds = new BasicDataSource();


    public TweetIntimacyMySQLBackend() {
        ds.setDriverClassName(JDBC_DRIVER);
        ds.setUrl(URL);
        ds.setUsername(DB_USER);
        ds.setPassword(DB_PWD);
        ds.setMinIdle(5);
        ds.setMaxIdle(20);
    }

    public ArrayList<ContactUser> query(Long userId, String phrase) {
        ArrayList<ContactUser> contacts = new ArrayList<>();

        // Get contact information
        final String sql = String.format(
                "SELECT uid, tweet_text, intimacy_score, "
                        + "screen_name, description, created_at FROM "
                        + "(SELECT user2_id AS uid, tweet_text, "
                        + "intimacy_score, created_at FROM contact_tweet "
                        + "WHERE user1_id=%d UNION "
                        + "SELECT user1_id AS uid, tweet_text, "
                        + "intimacy_score, created_at FROM contact_tweet "
                        + "WHERE user2_id=%d) AS tweet "
                        + "LEFT JOIN contact_user ON tweet.uid=contact_user.id "
                        + "ORDER BY uid ASC, created_at DESC",
                userId, userId);
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            Long lastUid = null;
            while (rs.next()) {
                Long uid = rs.getLong("uid");
                String text = rs.getString("tweet_text");
                double intimacyScore = rs.getDouble("intimacy_score");
                String screenName = rs.getString("screen_name");
                String desc = rs.getString("description");
                String createdAt = rs.getString("created_at");
                if (uid != lastUid) {
                    contacts.add(new ContactUser(uid, screenName,
                            desc, intimacyScore));
                    lastUid = uid;
                }
                contacts.get(contacts.size() - 1).addTweet(text, phrase, createdAt);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Sort contacts
        Collections.sort(contacts);
        return contacts;
    }
}
