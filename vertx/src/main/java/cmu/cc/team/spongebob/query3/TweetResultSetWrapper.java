package cmu.cc.team.spongebob.query3;

interface TweetResultSetWrapper {
    Tweet next();
    void close();
}
