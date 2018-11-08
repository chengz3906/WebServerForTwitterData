package cmu.cc.team.spongebob.query2.database;


import lombok.Getter;

import java.util.PriorityQueue;

public class ContactUser implements Comparable<ContactUser> {
    private @Getter Long userId;
    private @Getter String userName;
    private @Getter String userDescription;
    private PriorityQueue<ContactTweet> tweets;
    private int phraseScore;
    private double intimacyScore;
    private double score;

    public ContactUser(Long userId, String userName,
                   String userDescription, double intimacyScore) {
        this.userId = userId;
        this.userName = userName == null ? "" : userName;
        this.userDescription = userDescription == null ? ""
                : userDescription;
        this.phraseScore = 0;
        this.intimacyScore = intimacyScore;
        this.score = -1;
        this.tweets = new PriorityQueue<>();
    }

    public void addTweet(String text, String phrase, String createdAt) {
        ContactTweet tweet = new ContactTweet(text, phrase, createdAt);
        int phraseCount = tweet.getPhraseCount();
        this.phraseScore += phraseCount;
        this.tweets.add(tweet);
    }

    public String getTweetText() {
        return tweets.peek().getText();
    }

    public double getScore() {
        if (score < 0) {
            score = intimacyScore * (phraseScore + 1);
        }
        return score;
    }

    @Override
    public int compareTo(ContactUser other) {
        if (this.getScore() > other.getScore()) {
            return -1;
        }
        if (this.getScore() < other.getScore()) {
            return 1;
        }
        if (this.userId < other.userId) {
            return -1;
        }
        if (this.userId > other.userId) {
            return 1;
        }
        return 0;
    }
}