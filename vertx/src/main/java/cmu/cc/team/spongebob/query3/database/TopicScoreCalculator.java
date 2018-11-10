package cmu.cc.team.spongebob.query3.database;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.lang.Math;
import java.util.Collections;
import java.util.HashMap;
import java.util.ArrayList;

public class TopicScoreCalculator {

    private static final String pattern = "(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*";

    public static String getTopicScore(TweetResultSetWrapper rs, int n1, int n2) {
        StringBuilder resultBuilder = new StringBuilder();
        HashMap<String, Word> words = new HashMap<>();
        HashMap<Long, Tweet> tweets = new HashMap<>();
        ArrayList<Tweet> filteredTweets = new ArrayList<>();

        // Extract words from tweets
        while (rs.hasNext()) {
            Tweet tweet = rs.next();
            String text = tweet.getText();
            long tweetId = tweet.getTweetId();
            double impactScore = tweet.getImpactScore();
            tweets.put(tweetId, tweet);

            ArrayList<String> ws = extractWords(text);
            for (String w : ws) {
                w = w.toLowerCase();
                if (!words.containsKey(w)) {
                    words.put(w, new Word(w));
                }
                words.get(w).addTweet(tweetId, impactScore, ws.size());
            }
        }
        int tweetCount = tweets.size();
        ArrayList<Word> wordList = new ArrayList<>(words.values());
        for (Word word : wordList) {
            word.setTotalTweetCount(tweetCount);
            word.calculateTopicScore();
        }
        Collections.sort(wordList);

        // Get n1 words and n2 tweets
        n1 = Math.min(wordList.size(), n1);
        for (int i = 0; i < n1; ++i) {
            ArrayList<Long> tweetIds = wordList.get(i).getRelevantTweetId();
            for (Long id : tweetIds) {
                tweets.get(id).chosen = true;
            }
        }
        for (Tweet tweet : tweets.values()) {
            if (tweet.chosen) {
                filteredTweets.add(tweet);
            }
        }
        Collections.sort(filteredTweets);
        n2 = Math.min(filteredTweets.size(), n2);

        // Form response string
        for (int i = 0; i < n1; ++i) {
            Word word = wordList.get(i);
            resultBuilder.append(String.format("%s:%.2f",
                    word.getWord(), word.getTopicScore()));
            if (i < n1 - 1) {
                resultBuilder.append("\t");
            }
        }
        resultBuilder.append("\n");
        for (int i = 0; i < n2; ++i) {
            Tweet tweet = filteredTweets.get(i);
            resultBuilder.append(String.format("%d\t%d\t%s",
                    (int)tweet.impactScore, tweet.tweetId, tweet.censoredText));
            if (i < n2 - 1) {
                resultBuilder.append("\n");
            }
        }
        return resultBuilder.toString();
    }

    public static ArrayList<String> extractWords(String text) {
        String noUrl = text.replaceAll(pattern, "");
        ArrayList<String> ws = new ArrayList<>();

        boolean alphabet = false;
        int start = 0;
        int len = noUrl.length();
        for (int i = 0; i < len; ++i) {
            char c = text.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
                alphabet = true;
            } else if (c != '-' && c != '\'' && (c < '0' || c > '9')) {
                if (start != i && alphabet) {
                    ws.add(noUrl.substring(start, i));
                }
                alphabet = false;
                start = i + 1;
            }
        }
        if (start != len && alphabet) {
            ws.add(noUrl.substring(start, len));
        }
        return ws;
    }
}
