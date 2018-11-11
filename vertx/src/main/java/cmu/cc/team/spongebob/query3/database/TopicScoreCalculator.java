package cmu.cc.team.spongebob.query3.database;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.tuple.MutablePair;
import java.lang.Math;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TopicScoreCalculator {

    private static final String shortURLRegex = "(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*";

    @Data
    @AllArgsConstructor
    private static class TopicWord implements Comparable<TopicWord> {
        private String word;
        private double score;

        @Override
        public int compareTo(TopicWord other) {
            if (this.score > other.score) {
                return -1;
            }
            if (this.score < other.score) {
                return 1;
            }
            return this.word.compareTo(other.word);
        }
    }

    public static String getTopicScore(TweetResultSetWrapper rs, int n1, int n2) {
        StringBuilder resultBuilder = new StringBuilder();
        // HashMap<String, Word> words = new HashMap<>();
        HashMap<String, MutablePair<Double, HashSet<Long>>> wordsHashMap = new HashMap<>();
        HashMap<Long, Tweet> tweets = new HashMap<>();
        ArrayList<Tweet> filteredTweets = new ArrayList<>();
        HashSet<String> stopWords = new HashSet<>();

        // Extract words from tweets
        int numTweets = 0;
        for (Tweet tweet = rs.next(); tweet != null; tweet = rs.next()) {
            String text = tweet.getText();
            long tweetId = tweet.getTweetId();
            double impactScore = tweet.getImpactScore();
            tweets.put(tweetId, tweet);

            ArrayList<String> ws = extractWords(text);
            int numWords = ws.size();
            // HashSet<String> seenWords = new HashSet<>();
            double logImpactScore = Math.log(impactScore + 1) / numWords;
            for (String w : ws) {
                w = w.toLowerCase();

                if (!stopWords.contains(w)) {
                    if (!wordsHashMap.containsKey(w)) {
                        HashSet<Long> tweetIds = new HashSet<>();
                        tweetIds.add(tweetId);
                        MutablePair<Double, HashSet<Long>> counter = new MutablePair<>(logImpactScore, tweetIds);
                        wordsHashMap.put(w, counter);
                    } else {
                        MutablePair<Double, HashSet<Long>> counter = wordsHashMap.get(w);
                        counter.setLeft(counter.getLeft() + logImpactScore);
                        counter.getRight().add(tweetId);
                    }
                }
            }
            numTweets++;
        }

        PriorityQueue<TopicWord> topicWords = new PriorityQueue<>();
        for (Map.Entry<String, MutablePair<Double, HashSet<Long>>> kvPair: wordsHashMap.entrySet()) {
            double topicScore = Math.log((double) numTweets / kvPair.getValue().right.size()) * kvPair.getValue().left;
            topicWords.add(new TopicWord(kvPair.getKey(), topicScore));
        }
        
        // TODO pick top n1 topics words


        rs.close();
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
        for (Tweet t : tweets.values()) {
            if (t.chosen) {
                filteredTweets.add(t);
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
            Tweet t = filteredTweets.get(i);
            resultBuilder.append(String.format("%d\t%d\t%s",
                    (int)t.impactScore, t.tweetId, t.censoredText));
            if (i < n2 - 1) {
                resultBuilder.append("\n");
            }
        }
        return resultBuilder.toString();
    }

    public static ArrayList<String> extractWords(String text) {
        String noUrl = text.replaceAll(shortURLRegex, "");
        ArrayList<String> ws = new ArrayList<>();

        /*
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
        */

        Pattern pattern = Pattern.compile("([A-Za-z0-9'-]*[a-zA-Z][A-Za-z0-9'-]*)");
        String textNoUrl = text.replaceAll(shortURLRegex, "");
        Matcher matcher = pattern.matcher(textNoUrl);
        while(matcher.find()) {
            String word = matcher.group(1);
            ws.add(word);
        }
        return ws;
    }

}
