package cmu.cc.team.spongebob.query2.database;


import lombok.Getter;

import java.util.ArrayList;

public class ContactUser {
    private @Getter Long userId;
    private @Getter String userName;
    private @Getter String userDescription;
    private ArrayList<ContactTweet> tweets;
    private int phraseScore;
    private double intimacyScore;
    private double score;
    private int targetTweetIndex;
    private int targetTweetCount;

    public ContactUser(Long userId, String userName,
                   String userDescription, double intimacyScore) {
        this.userId = userId;
        this.userName = userName == null ? "" : userName;
        this.userDescription = userDescription == null ? ""
                : userDescription;
        this.phraseScore = 0;
        this.targetTweetIndex = -1;
        this.targetTweetCount = 0;
        this.intimacyScore = intimacyScore;
        this.score = -1;
        this.tweets = new ArrayList<>();
    }

    public void addTweet(String text, String phrase) {
        ContactTweet tweet = new ContactTweet(text, phrase);
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
        if (score < 0) {
            score = intimacyScore * (phraseScore + 1);
        }
        return score;
    }
}