package cmu.cc.team.spongebob.query3;

import java.lang.Math;
import java.util.ArrayList;
import lombok.Getter;
import lombok.Setter;


public class Word implements Comparable<Word> {

    private @Getter String word;
    private @Getter double topicScore;
    private ArrayList<Integer> relevantTweetAppearCount;
    private ArrayList<Integer> relevantTweetWordCount;
    private ArrayList<Double> relevantTweetImpactScore;
    private @Getter ArrayList<Long> relevantTweetId;
    private long currentTweet;
    private @Setter int totalTweetCount;
    private int relevantTweetCount;

    public Word(String word) {
        this.word = word;
        this.relevantTweetAppearCount = new ArrayList<>();
        this.relevantTweetWordCount = new ArrayList<>();
        this.relevantTweetImpactScore = new ArrayList<>();
        this.relevantTweetId = new ArrayList<>();
        this.relevantTweetCount = 0;
        this.currentTweet = -1;
        this.topicScore = -1;
    }

    public void addTweet(long tweetId, double impactScore, int wordCountInTweet) {
        if (tweetId != currentTweet) {
            this.relevantTweetAppearCount.add(0);
            this.relevantTweetWordCount.add(wordCountInTweet);
            this.relevantTweetImpactScore.add(impactScore);
            this.relevantTweetId.add(tweetId);
            this.currentTweet = tweetId;
            this.relevantTweetCount++;
        }
        this.relevantTweetAppearCount.set(this.relevantTweetCount - 1,
                this.relevantTweetAppearCount.get(this.relevantTweetCount - 1) + 1);
    }

    public void calculateTopicScore() {
        double topicScore = 0;
        for (int i = 0; i < relevantTweetCount; ++i) {
            double tf = (double)relevantTweetAppearCount.get(i)
                    / (double)relevantTweetWordCount.get(i);
            double idf = Math.log((double)totalTweetCount / (double)relevantTweetCount);
            topicScore += tf * idf * Math.log(relevantTweetImpactScore.get(i));
        }
        this.topicScore = topicScore;
    }

    @Override
    public int compareTo(Word other) {
        if (this.topicScore > other.topicScore) {
            return -1;
        } else if(this.topicScore < other.topicScore) {
            return 1;
        } else {
            return this.word.compareTo(other.word);
        }
    }
}
