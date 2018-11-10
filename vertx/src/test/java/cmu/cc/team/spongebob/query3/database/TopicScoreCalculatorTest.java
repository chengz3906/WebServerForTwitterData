package cmu.cc.team.spongebob.query3.database;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.ArrayList;
import java.lang.Math;

public class TopicScoreCalculatorTest {

    @Test
    void testExtractWord() {
        String[] strs = {"cloud computing is awesome 233",
                "please give me m-o-r-e cloud'computing",
                "another tweet with impact score as 3"};
        ArrayList<ArrayList<String>> res = new ArrayList<>();
        for (String s : strs) {
            res.add(TopicScoreCalculator.extractWords(s));
        }
        assertEquals(4, res.get(0).size());
        assertEquals(5, res.get(1).size());
        assertEquals(6, res.get(2).size());
        assertEquals("cloud'computing", res.get(1).get(4));
    }

    @Test
    void testTopicScore() {
        MyResultSet rs = new MyResultSet();
        String res = TopicScoreCalculator.getTopicScore(rs, 3, 2);
        assertEquals("me:3.98\tawesome:2.21\tcloud:2.21\n"
                +"53\t2\tplease give me me m-o-r-e cloud'computing\n"
                +"19\t1\tcloud computing is a*****e 233", res);
    }
}

class MyResultSet implements TweetResultSetWrapper {
    private ArrayList<Tweet> tweets = new ArrayList<>(Arrays.asList(
            new Tweet("cloud computing is awesome 233",
                    "cloud computing is a*****e 233", 1l, Math.exp(3)-1),
            new Tweet("please give me me m-o-r-e cloud'computing",
                    "please give me me m-o-r-e cloud'computing", 2l, Math.exp(4)-1),
            new Tweet("another tweet with impact score as 3",
                    "another tweet with impact score as 3", 3l, Math.exp(3)-1)
            ));
    private int i = 0;

    @Override
    public Tweet next() {
        return tweets.get(i++);
    }

    @Override
    public boolean hasNext() {
        return i < tweets.size();
    }
}