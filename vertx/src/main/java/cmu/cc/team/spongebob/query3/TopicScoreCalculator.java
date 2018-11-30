package cmu.cc.team.spongebob.query3;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.tuple.MutablePair;

import java.io.*;
import java.lang.Math;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TopicScoreCalculator {

    private static final Pattern pattern = Pattern.compile("([A-Za-z0-9'-]*[a-zA-Z][A-Za-z0-9'-]*)");
    private static final String shortURLRegex = "(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*";
    private static HashSet<String> stopWords;
    private static HashMap<String, String> censorDict;

    @Data
    @AllArgsConstructor
    private class TopicWord implements Comparable<TopicWord> {
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

    public TopicScoreCalculator() {
        stopWords = new HashSet<>();
        censorDict = new HashMap<>();
        loadStopWords();
        loadCensorDict();
    }

    private void loadStopWords() {
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream in = classLoader.getResourceAsStream("stopwords.txt");

        try (Scanner scanner = new Scanner(in)) {
            while (scanner.hasNextLine()) {
                String stopWord = scanner.nextLine().toLowerCase();
                stopWords.add(stopWord);
            }
        }
    }

    private void loadCensorDict() {
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream in = classLoader.getResourceAsStream("censored_dict.txt");

        try (Scanner scanner = new Scanner(in)) {
            while (scanner.hasNextLine()) {
                String[] censorPair = scanner.nextLine().toLowerCase().split(",");
                censorDict.put(censorPair[0], censorPair[1]);
            }
        }
    }

    public String getTopicScore(TweetResultSetWrapper rs, int n1, int n2) {
        StringBuilder resultBuilder = new StringBuilder();
        HashMap<String, MutablePair<Double, HashSet<Long>>> wordsHashMap = new HashMap<>();
        HashMap<Long, Tweet> tweets = new HashMap<>();
        ArrayList<Tweet> filteredTweets = new ArrayList<>();

        // Extract words from tweets
        int numTweets = 0;
        for (Tweet tweet = rs.next(); tweet != null; tweet = rs.next()) {
            String text = tweet.getText();
            long tweetId = tweet.getTweetId();
            double impactScore = tweet.getImpactScore();
            tweets.put(tweetId, tweet);

            ArrayList<String> ws = extractWords(text);
            int numWords = ws.size();
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

        ArrayList<TopicWord> topicWords = new ArrayList<>();
        for (Map.Entry<String, MutablePair<Double, HashSet<Long>>> kvPair: wordsHashMap.entrySet()) {
            double topicScore = Math.log((double) numTweets / kvPair.getValue().right.size()) * kvPair.getValue().left;
            topicWords.add(new TopicWord(kvPair.getKey(), topicScore));
        }
        Collections.sort(topicWords);

        // Get n1 words and n2 tweets
        n1 = Math.min(topicWords.size(), n1);
        for (int i = 0; i < n1; ++i) {
            HashSet<Long> tweetIds = wordsHashMap.get(topicWords.get(i).word).getRight();
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
            TopicWord word = topicWords.get(i);
            String censoredWord = word.word;
            if (censorDict.containsKey(censoredWord)) {
                censoredWord = censorDict.get(censoredWord);
            }
            resultBuilder.append(String.format("%s:%.2f",
                    censoredWord, word.score));
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

    public ArrayList<String> extractWords(String text) {
        ArrayList<String> ws = new ArrayList<>();
        String textNoUrl = text.replaceAll(shortURLRegex, "");
        Matcher matcher = pattern.matcher(textNoUrl);
        while(matcher.find()) {
            String word = matcher.group(1);
            ws.add(word);
        }
        return ws;
    }

}
