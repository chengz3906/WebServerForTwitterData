package cmu.cc.team.spongebob.query2.database;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TweetIntimacyMySQLBackendTest {

    private final TweetIntimacyMySQLBackend dbReader = new TweetIntimacyMySQLBackend();

    @Test
    void testQuery() {
        Long userId = 492532196l;
        String phrase = "Spend";
        int n = 5;
        dbReader.query(userId, phrase);
//        for (int i = 0; i < n; ++i) {
//            System.out.println(String.format("%s,%s,%s",
//                    userName.get(i), userDesc.get(i),
//                    contactTweet.get(i)));
//        }
//        assertEquals(userName.size(), userDesc.size());
//        assertEquals(userName.size(), contactTweet.size());
//        assertEquals("@bratcute1234 Thx for enrolling in #AmexWestElm offer. Spend w/connected Card &amp; receive credit. Terms: http://t.co/Fi7wuHocPP",
//                contactTweet.get(0));
    }


    @Test
    void testContact() {
        ContactUser contact = new ContactUser(1l,"a", "aa", 5.67);
        contact.addTweet("It's my life-style.", "life", "1");
        contact.addTweet("Cloud+computing=life", "life", "2");
        contact.addTweet("It's my life.", "life", "3");
        contact.addTweet("That's my life, I think.", "life", "2");
        contact.addTweet("life is good", "life", "2");
        contact.addTweet("it's my life", "life", "2");
        contact.addTweet("that's my life lesson", "life", "2");
        contact.addTweet("cloud computing is soooo awesome!!!", "cloud computing", "2");
        contact.addTweet("please give me more cloud computing! I really need cloud computing!!", "cloud computing", "2");
        assertEquals(5.67*5, contact.getScore());
        assertEquals("life is good", contact.getTweetText());
        assertEquals("a", contact.getUserName());
        assertEquals("aa", contact.getUserDescription());

    }

}