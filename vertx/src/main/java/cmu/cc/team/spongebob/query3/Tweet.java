package cmu.cc.team.spongebob.query3;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Data
public class Tweet implements Comparable<Tweet> {

    String text;
    long tweetId;
    double impactScore;
    boolean chosen;
    static Pattern pat = Pattern.compile("[A-Za-z0-9]+");

    public Tweet(String text, long tweetId, double impactScore) {
        this.text = text;
        this.tweetId = tweetId;
        this.impactScore = impactScore;
        this.chosen = false;
    }

    @Override
    public int compareTo(Tweet other) {
        if (this.impactScore > other.impactScore) {
            return 1;
        }
        if (this.impactScore < other.impactScore) {
            return -1;
        }
        if (this.tweetId > other.tweetId) {
            return 1;
        }
        return -1;
    }

    public String censorText(HashMap<String, String> censoredWords) {
        Matcher mat = pat.matcher(text);
        String censored = text;
        while (mat.find()) {
            String token = mat.group(0);
            if (censoredWords.get(token.toLowerCase()) != null) {
                String censoredWord =
                        token.charAt(0) + StringUtils.repeat("*", (token.length() - 2)) + token.charAt(token.length() - 1);
                censored =
                        censored.substring(0, mat.start()) + censoredWord + censored.substring(mat.end());
            }
        }
        return censored;
    }

}
