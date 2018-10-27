package cmu.cc.team.spongebob.query2.database;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.commons.dbcp2.BasicDataSource;

public class DBReader {
    /**
     * JDBC driver of MySQL Connector/J.
     */
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    /**
     * Database name.
     */
    private static final String DB_NAME = "twitter";
    /**
     * DNS of Mysql database
     */
    private static final String DNS = "35.237.23.247";
    /**
     * Database url
     */
    private static final String URL = "jdbc:mysql://" + DNS + "/" + DB_NAME
            + "?useSSL=false";
    /**
     * Username and password.
     */
//    private static final String DB_USER = System.getenv("MYSQL_USER");
//    private static final String DB_PWD = System.getenv("MYSQL_PWD");
    private static final String DB_USER = "root";
    private static final String DB_PWD = "Allen123";
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
        ArrayList<Long> contactIds = new ArrayList<>();
        HashMap<Long, Contact> contacts = new HashMap<>();
        ArrayList<Contact> filteredContacts;
        getContacts(userId, phrase, contactIds, contacts);
        filteredContacts = filterContact(contactIds, contacts, n);
        for (Contact contact : filteredContacts) {
            userName.add(contact.getUserName());
            userDesc.add(contact.getUserDescription());
            contactTweet.add(contact.getTweetText());
        }
    }

    public void getContacts(Long userId, String phrase,
                            ArrayList<Long> contactIds,
                            HashMap<Long, Contact> contacts) {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            String sql = "SELECT uid, tweet_text, intimacy_score, "
                    + "screen_name, description FROM "
                    + "(SELECT user2_id AS uid, tweet_text, "
                    + "intimacy_score, created_at FROM contact_tweet "
                    + "WHERE user1_id=" + userId + " UNION "
                    + "SELECT user1_id AS uid, tweet_text, "
                    + "intimacy_score, created_at FROM contact_tweet "
                    + "WHERE user2_id=" + userId + ") AS tweet "
                    + "LEFT JOIN contact_user ON tweet.uid=contact_user.id "
                    + "ORDER BY uid ASC, created_at DESC";
            Connection conn = ds.getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            Long lastUid = null;
            while (rs.next()) {
                Long uid = rs.getLong("uid");
                String text = rs.getString("tweet_text");
                double intimacyScore = rs.getDouble("intimacy_score");
                String screenName = rs.getString("screen_name");
                String desc = rs.getString("description");
                if (!uid.equals(lastUid)) {
                    contactIds.add(uid);
                    contacts.put(uid, new Contact(screenName,
                            desc, intimacyScore));
                    lastUid = uid;
                }
                contacts.get(uid).addTweet(text, phrase);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public ArrayList<Contact> filterContact(ArrayList<Long> contactIds,
                                            HashMap<Long, Contact> contacts, int n) {
        ArrayList<Contact> filteredContacts = new ArrayList<>();
        for (Long uid : contactIds) {
            double score = contacts.get(uid).getScore();
            int index = 0;
            while (index < filteredContacts.size()
                    && score <= filteredContacts.get(index).getScore()) {
                index++;
            }
            filteredContacts.add(index, contacts.get(uid));
            if (filteredContacts.size() > n) {
                filteredContacts.remove(n);
            }
        }
        return filteredContacts;
    }

    public class Contact {
        private String userName;
        private String userDescription;
        private ArrayList<Tweet> tweets;
        private int phraseScore;
        private double intimacyScore;
        private int targetTweetIndex;
        private int targetTweetCount;

        public Contact(String userName, String userDescription,
                       double intimacyScore) {
            this.userName = userName == null ? "" : userName;
            this.userDescription = userDescription == null ? ""
                    : userDescription;
            this.phraseScore = 0;
            this.targetTweetIndex = -1;
            this.targetTweetCount = 0;
            this.intimacyScore = intimacyScore;
            this.tweets = new ArrayList<>();
        }

        public void addTweet(String text, String phrase) {
            Tweet tweet = new Tweet(text, phrase);
            int phraseCount = tweet.getPhraseCount();
            this.phraseScore += phraseCount;
            if (phraseCount > this.targetTweetCount) {
                this.targetTweetCount = phraseCount;
                this.targetTweetIndex = tweets.size();
            }
            this.tweets.add(tweet);
        }

        public String getTweetText() {
            if (targetTweetIndex >= 0) {
                return this.tweets.get(targetTweetIndex).getText();
            } else {
                return this.tweets.get(0).getText();
            }
        }

        public double getScore() {
            return intimacyScore * (phraseScore + 1);
        }

        public String getUserName() {
            return userName;
        }

        public String getUserDescription() {
            return userDescription;
        }

        private class Tweet {
            private String text;
            private int phraseCount;

            public Tweet(String text, String phrase) {
                this.text = text;
                countPhrase(phrase);
            }

            private void countPhrase(String phrase) {
                int index = 0;
                int count = 0;
                index = text.indexOf(phrase, index);
                while (index != -1) {
                    int lindex = index - 1;
                    int rindex = index + phrase.length();
                    if ((lindex < 0 || text.charAt(lindex) == ' ')
                        && (rindex >= text.length() || text.charAt(rindex) == ' ')) {
                        count++;
                    }
                    index += phrase.length();
                    index = text.indexOf(phrase, index);
                }
                phraseCount = count;
            }

            public String getText() {
                return text;
            }

            public int getPhraseCount() {
                return phraseCount;
            }
        }
    }
}
