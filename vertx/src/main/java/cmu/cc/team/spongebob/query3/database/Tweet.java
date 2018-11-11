package cmu.cc.team.spongebob.query3.database;

import lombok.Data;

@Data
public class Tweet implements Comparable<Tweet> {

    String text;
    String censoredText;
    long tweetId;
    double impactScore;
    boolean chosen;

    public Tweet(String text, String censoredText, long tweetId,
                 double impactScore) {
        this.text = text;
        this.censoredText = censoredText;
        this.tweetId = tweetId;
        this.impactScore = impactScore;
        this.chosen = false;
    }

    @Override
    public int compareTo(Tweet other) {
        if (this.impactScore > other.impactScore) {
            return -1;
        }
        if (this.impactScore < other.impactScore) {
            return 1;
        }
        if (this.tweetId > other.tweetId) {
            return -1;
        }
        return 1;
    }
}
