package cmu.cc.team.spongebob.query2;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.PriorityQueue;

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

    public ArrayList<ContactUser> query(Long userId, String phrase, int n) {
        ArrayList<ContactUser> contacts = new ArrayList<>();

        // Get contact information
        final String sql = "SELECT user2_id, tweet_text, intimacy_score, "
                + "user2_screen_name, user2_desc, created_at FROM "
                + "contact_tweet WHERE user1_id=? "
                + "ORDER BY user2_id ASC, created_at DESC";
        try (Connection conn = ds.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            Long lastUid = null;
            stmt.setLong(1, userId);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                Long uid = rs.getLong("user2_id");
                String text = rs.getString("tweet_text");
                double intimacyScore = rs.getDouble("intimacy_score");
                String screenName = rs.getString("user2_screen_name");
                String desc = rs.getString("user2_desc");
                Long createdAt = rs.getLong("created_at");
                if (!uid.equals(lastUid)) {
                    contacts.add(new ContactUser(uid, screenName,
                            desc, intimacyScore));
                    lastUid = uid;
                }
                contacts.get(contacts.size() - 1).addTweet(text, phrase, createdAt);
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Sort contacts
        PriorityQueue<ContactUser> sortedContacts = new PriorityQueue<>();
        for (ContactUser cu : contacts) {
            sortedContacts.add(cu);
            if (sortedContacts.size() > n) {
                sortedContacts.poll();
            }
        }
        ArrayList<ContactUser> reversedContacts = new ArrayList<>();
        while (!sortedContacts.isEmpty()) {
            reversedContacts.add(0, sortedContacts.poll());
        }
        return reversedContacts;
    }
}
