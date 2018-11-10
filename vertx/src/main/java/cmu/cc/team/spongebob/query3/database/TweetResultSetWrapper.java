package cmu.cc.team.spongebob.query3.database;

interface TweetResultSetWrapper {
    Tweet next();
    boolean hasNext();
}
