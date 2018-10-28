package cmu.cc.team.spongebob.query2.database;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class DBReaderTest {

    private final DBReader dbReader = new DBReader();

    @Test
    void testQuery() {
        Long userId = 492532196l;
        String phrase = "Spend";
        int n = 5;
        ArrayList<String> userName = new ArrayList<>();
        ArrayList<String> userDesc = new ArrayList<>();
        ArrayList<String> contactTweet = new ArrayList<>();
        dbReader.query(userId, phrase, n, userName, userDesc, contactTweet);
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
    void testGetContacts() {
        Long userId = 2392956788l;
        Long cid0 = 1705703466l;
        Long cid1 = 2177883343l;
        String phrase = "FOLLOW";
        ArrayList<ContactUser> contacts = new ArrayList<>();
        dbReader.getContacts(userId, phrase, contacts);
//        assertEquals(2, contacts.size());
//        assertEquals(cid0, contacts.get(0).getUserId());
//        assertEquals(cid1, contacts.get(1).getUserId());
//        assertEquals(1.0986122886681098 * 2, contacts.get(0).getScore());
//        assertEquals("RT @_o_MARIELLE_o_: #RETWEET THIS! FOLLOW ALL WHO RT FOR 25+ FOLLOWERS! #TeamFollowback #FollowTrick #MGWV #AnotherFollowTrain\n" +
//                "\n" +
//                "#FOLLOW ☞ @…", contacts.get(0).getTweetText());
//        userId = 492532196l;
//        contacts = new ArrayList<>();
//        dbReader.getContacts(userId, phrase, contacts);
//        assertEquals("", contacts.get(0).getUserName());
//        assertEquals("", contacts.get(0).getUserDescription());
    }


    @Test
    void testContact() {
        ContactUser contact = new ContactUser(1l,"a", "aa", 5.67);
        contact.addTweet("It's my life-style.", "life");
        contact.addTweet("Cloud+computing=life", "life");
        contact.addTweet("It's my life.", "life");
        contact.addTweet("That's my life, I think.", "life");
        contact.addTweet("life is good", "life");
        contact.addTweet("it's my life", "life");
        contact.addTweet("that's my life lesson", "life");
        contact.addTweet("cloud computing is soooo awesome!!!", "cloud computing");
        contact.addTweet("please give me more cloud computing! I really need cloud computing!!", "cloud computing");
        assertEquals(5.67*5, contact.getScore());
        assertEquals("life is good", contact.getTweetText());
        assertEquals("a", contact.getUserName());
        assertEquals("aa", contact.getUserDescription());

    }

    @Test
    void testFilterContact() {
        ArrayList<Long> cids = new ArrayList<>(Arrays.asList(1l,2l,3l,4l,5l));
        ArrayList<ContactUser> cs = new ArrayList<>();
        ArrayList<String> tweets = new ArrayList<>();
        for (Long id : cids) {
            cs.add(new ContactUser(id,"a"+id, "aa"+id, (10l-id)*(10l-id)));
            String tweet = "";
            for (int i = 0; i < id; ++i) {
                tweet += "cloud ";
            }
            cs.get(cs.size() - 1).addTweet(tweet, "cloud");
            tweets.add(tweet);
        }
        dbReader.sortContact(cs);
        assertEquals(tweets.get(2), cs.get(0).getTweetText());
        assertEquals(tweets.get(1), cs.get(1).getTweetText());
        assertEquals(tweets.get(3), cs.get(2).getTweetText());
    }
}
