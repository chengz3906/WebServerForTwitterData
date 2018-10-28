package cmu.cc.team.spongebob.query2.database;

import java.sql.*;
import java.util.ArrayList;
import org.apache.commons.dbcp2.BasicDataSource;

public class DBReader {
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


    public DBReader() {
        ds.setDriverClassName(JDBC_DRIVER);
        ds.setUrl(URL);
        ds.setUsername(DB_USER);
        ds.setPassword(DB_PWD);
        ds.setMinIdle(5);
        ds.setMaxIdle(20);
    }

    public void query(Long userId, String phrase, int n,
                      ArrayList<String> userName, ArrayList<String> userDesc,
                      ArrayList<String> contactTweet) {
        ArrayList<ContactUser> contacts = new ArrayList<>();
        getContacts(userId, phrase, contacts);
        sortContact(contacts);
        n = n > contacts.size() ? contacts.size() : n;
        for (int i = 0; i < n; ++i) {
            userName.add(contacts.get(i).getUserName());
            userDesc.add(contacts.get(i).getUserDescription());
            contactTweet.add(contacts.get(i).getTweetText());
        }
    }

    public void getContacts(Long userId, String phrase,
                            ArrayList<ContactUser> contacts) {
        final String sql = String.format(
                "SELECT uid, tweet_text, intimacy_score, "
                + "screen_name, description FROM "
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
             ResultSet rs = stmt.executeQuery(sql)
        ) {
            Long lastUid = null;
            while (rs.next()) {
                Long uid = rs.getLong("uid");
                String text = rs.getString("tweet_text");
                double intimacyScore = rs.getDouble("intimacy_score");
                String screenName = rs.getString("screen_name");
                String desc = rs.getString("description");
                if (uid != lastUid) {
                    contacts.add(new ContactUser(uid, screenName,
                            desc, intimacyScore));
                    lastUid = uid;
                }
                contacts.get(contacts.size() - 1).addTweet(text, phrase);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void sortContact(ArrayList<ContactUser> contacts) {

        contacts.sort((o1, o2) -> {
            if (o1.getScore() > o2.getScore()) {
                return -1;
            }
            else if (o1.getScore() < o2.getScore()) {
                return 1;
            }
            else if (o1.getUserId() < o2.getUserId()) {
                return -1;
            }
            else return 1;
        });
    }
}
