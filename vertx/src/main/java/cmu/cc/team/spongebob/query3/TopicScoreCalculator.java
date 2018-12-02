package cmu.cc.team.spongebob.query3;

import io.vertx.ext.sql.SQLRowStream;
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
                return 1;
            }
            if (this.score < other.score) {
                return -1;
            }
            return other.word.compareTo(this.word);
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

    public String getTopicScore(SQLRowStream sqlRowStream, int n1, int n2) {
        StringBuilder resultBuilder = new StringBuilder();
        HashMap<String, MutablePair<Double, HashSet<Long>>> wordsHashMap = new HashMap<>();
        HashMap<Long, Tweet> tweets = new HashMap<>();
        PriorityQueue<Tweet> filteredTweets = new PriorityQueue<>();
        ArrayList<Tweet> reversedTweets = new ArrayList<>();
        PriorityQueue<TopicWord> topicWords = new PriorityQueue<>();
        ArrayList<TopicWord> reversedWords = new ArrayList<>();
        // Extract words from tweets
        sqlRowStream
                .resultSetClosedHandler(v -> {
                    sqlRowStream.moreResults();
                })
                .handler(row -> {
                    long tweetId = row.getLong(0);
                    String text = row.getString(1);
                    String censoredText = row.getString(2);
                    double impactScore = row.getDouble(3);
                    tweets.put(tweetId, new Tweet(text, censoredText, tweetId, impactScore));

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
                })
                .endHandler(v -> {});

        int numTweets = tweets.size();
        for (Map.Entry<String, MutablePair<Double, HashSet<Long>>> kvPair: wordsHashMap.entrySet()) {
            double topicScore = Math.log((double) numTweets / kvPair.getValue().right.size()) * kvPair.getValue().left;
            topicWords.add(new TopicWord(kvPair.getKey(), topicScore));
            if (topicWords.size() > n1) {
                topicWords.poll();
            }
        }
        while (!topicWords.isEmpty()) {
            reversedWords.add(0, topicWords.poll());
        }

        // Get n1 words and n2 tweets
        for (TopicWord w : reversedWords) {
            HashSet<Long> tweetIds = wordsHashMap.get(w.word).getRight();
            for (Long id : tweetIds) {
                tweets.get(id).chosen = true;
            }
        }
        for (Tweet t : tweets.values()) {
            if (t.chosen) {
                filteredTweets.add(t);
                if (filteredTweets.size() > n2) {
                    filteredTweets.poll();
                }
            }
        }
        while (!filteredTweets.isEmpty()) {
            reversedTweets.add(0, filteredTweets.poll());
        }
        // Form response string
        for (TopicWord word : reversedWords) {
            String censoredWord = word.word;
            if (censorDict.containsKey(censoredWord)) {
                censoredWord = censorDict.get(censoredWord);
            }
            resultBuilder.append(String.format("%s:%.2f\t",
                    censoredWord, word.score));
        }
        resultBuilder.deleteCharAt(resultBuilder.length() - 1);
        resultBuilder.append("\n");
        for (Tweet t : reversedTweets) {
            resultBuilder.append(String.format("%d\t%d\t%s\n",
                    (int)t.impactScore, t.tweetId, t.censoredText));
        }
        resultBuilder.deleteCharAt(resultBuilder.length() - 1);
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
