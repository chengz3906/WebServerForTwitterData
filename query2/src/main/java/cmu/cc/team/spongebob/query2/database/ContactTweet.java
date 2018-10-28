package cmu.cc.team.spongebob.query2.database;


import lombok.Getter;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class ContactTweet {
    private @Getter String text;
    private @Getter int phraseCount;

    public ContactTweet(String text, String phrase) {
        this.text = text;
        countPhrase(phrase);
    }

    private void countPhrase(String phrase) {
        int index = 0;
        int count = 0;
        index = text.indexOf(phrase, index);
        String regex = String.format("^\\s?%s\\s?$", phrase);
        Pattern pattern = Pattern.compile(regex);
        while (index != -1) {
            int lindex = index - 1;
            int rindex = index + phrase.length() + 1;
            lindex = lindex >= 0 ? lindex : 0;
            rindex = rindex <= text.length() ? rindex : text.length();
            String substr = text.substring(lindex, rindex);
            Matcher matcher = pattern.matcher(substr);
            if (matcher.find()) {
                count++;
            }
            index += phrase.length();
            index = text.indexOf(phrase, index);
        }
        phraseCount = count;
    }
//
//    public String getText() {
//        return text;
//    }
//
//    public int getPhraseCount() {
//        return phraseCount;
//    }
}
