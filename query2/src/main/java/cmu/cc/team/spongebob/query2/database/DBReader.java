package cmu.cc.team.spongebob.query2.database;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;


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
     * The connection (session) with the database.
     */
    private static Connection conn;

    public DBReader(){
        try {
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public void query(Long userId, String phrase, int n,
                      ArrayList<String> userName, ArrayList<String> userDesc,
                      ArrayList<String> contactTweet) {
        ArrayList<Long> contactTid = new ArrayList<>();
        ArrayList<Long> contactUid = new ArrayList<>();
        HashMap<Long, Contact> contacts;
        ArrayList<Contact> filteredContacts;
        getContacts(userId, contactTid, contactUid);
        contacts = genContactInfo(contactTid, contactUid, phrase);
        setIntimacyScore(contacts, userId);
        filteredContacts = filterContact(contacts, n);
        for (Contact contact : filteredContacts) {
            userName.add(contact.getUserName());
            userDesc.add(contact.getUserDescription());
            contactTweet.add(contact.getTweetText());
        }
    }

    public void getContacts(Long userId, ArrayList<Long> contactTid,
                             ArrayList<Long> contactUid) {
        try {
            String sql = "SELECT in_reply_to_user_id AS uid, tweet_id FROM reply "
                    + "WHERE user_id=" + userId + " UNION "
                    + "SELECT user_id AS uid, tweet_id FROM reply "
                    + "WHERE in_reply_to_user_id=" + userId + " UNION "
                    + "SELECT retweet_user_id AS uid, tweet_id FROM retweet ";
            ResultSet rs = executeQuery(sql);
            while (rs.next()) {
                contactTid.add(rs.getLong("tweet_id"));
                contactUid.add(rs.getLong("in_reply_to_user_id"));
            }

            sql = "SELECT user_id, tweet_id FROM reply "
                    + "WHERE in_reply_to_user_id=\"" + userId + "\"";
            rs = executeQuery(sql);
            while (rs.next()) {
                contactTid.add(rs.getLong("tweet_id"));
                contactUid.add(rs.getLong("user_id"));
            }

            sql = "SELECT retweet_user_id, tweet_id FROM retweet "
                    + "WHERE user_id=\"" + userId + "\"";
            rs = executeQuery(sql);
            while (rs.next()) {
                contactTid.add(rs.getLong("tweet_id"));
                contactUid.add(rs.getLong("retweet_user_id"));
            }

            sql = "SELECT user_id, tweet_id FROM retweet "
                    + "WHERE retweet_user_id=\"" + userId + "\"";
            rs = executeQuery(sql);
            while (rs.next()) {
                contactTid.add(rs.getLong("tweet_id"));
                contactUid.add(rs.getLong("user_id"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public HashMap<Long, Contact> genContactInfo(ArrayList<Long> contactTid,
                                                    ArrayList<Long> contactUid, String phrase) {
        HashMap<Long, Contact> contacts = new HashMap<>();
        String sql;
        ResultSet rs;
        try {
            sql = "SELECT user_id, user_name, user_description "
                    + "FROM user WHERE user_id IN (";
            for (Long uid : contactUid) {
                sql += "\"" + uid + "\",";
            }
            sql = sql.substring(0, sql.length()-1) + ") "
                + "ORDER BY user_id ASC";
            rs = executeQuery(sql);
            while (rs.next()) {
                Long userId = rs.getLong("user_id");
                String userName = rs.getString("user_name");
                String userDescription = rs.getString("user_description");
                contacts.put(userId, new Contact(userId, userName, userDescription));
            }
            sql = "SELECT user_id, tweet_id, text, created_at "
                    + "FROM tweet WHERE tweet_id IN (";
            for (Long tid : contactTid) {
                sql += "\"" + tid + "\",";
            }
            sql = sql.substring(0, sql.length()-1) + ")";
            rs = executeQuery(sql);
            while (rs.next()) {
                Long userId = rs.getLong("user_id");
                Long tweetId = rs.getLong("tweet_id");
                String text = rs.getString("text");
                Timestamp createdAt = rs.getTimestamp("created_at");
                contacts.get(userId).addTweet(tweetId, text, createdAt, phrase);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return contacts;
    }

    public void setIntimacyScore(HashMap<Long, Contact> contacts, Long userId) {
        String sql;
        ResultSet rs;
        Long lid, rid;
        try {
            for (Long contactId : contacts.keySet()) {
                lid = contactId > userId ? userId : contactId;
                rid = contactId > userId ? contactId : userId;
                sql = "SELECT intimacy_score FROM intimacy "
                        +"WHERE user_id_1=\"" + lid + "\" "
                        +"AND user_id_2=\"" + rid + "\"";
                rs = executeQuery(sql);
                while (rs.next()) {
                    int intimacyScore = rs.getInt("intimacy_score");
                    contacts.get(contactId).setIntimacyScore(intimacyScore);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<Contact> filterContact(HashMap<Long, Contact> contacts, int n) {
        ArrayList<Contact> filteredContacts = new ArrayList<>();
        for (Long uid : contacts.keySet()) {
            int score = contacts.get(uid).getScore();
            int index = 0;
            while (index < filteredContacts.size()
                    && score < filteredContacts.get(index).getScore()) {
                index++;
            }
            filteredContacts.add(index, contacts.get(uid));
            if (filteredContacts.size() > n) {
                filteredContacts.remove(n);
            }
        }
        return filteredContacts;
    }

    private static ResultSet executeQuery(final String sql) {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
//        finally {
//            if (stmt != null) {
//                try {
//                    stmt.close();
//                } catch (SQLException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
        return rs;
    }

    private class Contact {
        private Long userId;
        private String userName;
        private String userDescription;
        private ArrayList<Tweet> tweets;
        private int phraseScore;
        private int intimacyScore;
        private int targetTweetIndex;
        private int targetTweetCount;

        public Contact(Long userId, String userName, String userDescription) {
            this.userId = userId;
            this.userName = userName;
            this.userDescription = userDescription;
            this.phraseScore = 0;
            this.targetTweetIndex = -1;
            this.targetTweetCount = 0;
        }

        public void addTweet(Long tweetId, String text,
                             Timestamp createdAt, String phrase) {
            Tweet tweet = new Tweet(tweetId, text, createdAt, phrase);
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
            }
            else {
                return this.tweets.get(0).getText();
            }
        }

        public void setIntimacyScore(int intimacyScore) {
            this.intimacyScore = intimacyScore;
        }

        public int getScore() {
            return intimacyScore * (phraseScore + 1);
        }

        public String getUserName() {
            return userName;
        }

        public String getUserDescription() {
            return userDescription;
        }

        private class Tweet {
            private Long tweetId;
            private String text;
            private Timestamp createdAt;
            private int phraseCount;

            public Tweet(Long tweetId, String text,
                         Timestamp createdAt, String phrase) {
                this.tweetId = tweetId;
                this.text = text;
                this.createdAt = createdAt;
                countPhrase(phrase);
            }

            private void countPhrase(String phrase) {
                int index = 0;
                int count = 0;
                index = text.indexOf(phrase, index);
                while (index != -1) {
                    count++;
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
