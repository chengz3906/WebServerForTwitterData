package cmu.cc.team.spongebob.query3;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class TweetTest {

    @Test
    void testCompare() {
        ArrayList<Tweet> tweets = new ArrayList<>(Arrays.asList(
                new Tweet("cloud computing is awesome 233",
                        "cloud computing is awesome 233", 1l, 3),
                new Tweet("please give me m-o-r-e cloud'computing",
                        "please give me m-o-r-e cloud'computing", 2l, 5),
                new Tweet("another tweet with impact score as 3",
                        "another tweet with impact score as 3", 3l, 3)
        ));
        Collections.sort(tweets);
        assertEquals("please give me m-o-r-e cloud'computing", tweets.get(2).text);
        assertEquals("another tweet with impact score as 3", tweets.get(1).text);
        assertEquals("cloud computing is awesome 233", tweets.get(0).text);
    }
}
