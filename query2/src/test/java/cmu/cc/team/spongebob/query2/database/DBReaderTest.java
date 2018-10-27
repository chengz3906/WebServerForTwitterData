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
        assertEquals(userName.size(), userDesc.size());
        assertEquals(userName.size(), contactTweet.size());
        assertEquals("@bratcute1234 Thx for enrolling in #AmexWestElm offer. Spend w/connected Card &amp; receive credit. Terms: http://t.co/Fi7wuHocPP",
                contactTweet.get(0));
    }

    @Test
    void testGetContacts() {
        Long userId = 2392956788l;
        Long cid0 = 1705703466l;
        Long cid1 = 2177883343l;
        String phrase = "FOLLOW";
        ArrayList<Long> contactIds = new ArrayList<>();
        HashMap<Long, DBReader.Contact> contacts = new HashMap<>();
        dbReader.getContacts(userId, phrase, contactIds, contacts);
        assertEquals(2, contactIds.size());
        assertEquals(cid0, contactIds.get(0));
        assertEquals(cid1, contactIds.get(1));
        assertEquals(1.0986122886681098 * 2, contacts.get(contactIds.get(0)).getScore());
        assertEquals("RT @_o_MARIELLE_o_: #RETWEET THIS! FOLLOW ALL WHO RT FOR 25+ FOLLOWERS! #TeamFollowback #FollowTrick #MGWV #AnotherFollowTrain\n" +
                "\n" +
                "#FOLLOW ☞ @…", contacts.get(contactIds.get(0)).getTweetText());
        userId = 492532196l;
        contactIds = new ArrayList<>();
        contacts = new HashMap<>();
        dbReader.getContacts(userId, phrase, contactIds, contacts);
        assertEquals("", contacts.get(contactIds.get(0)).getUserName());
        assertEquals("", contacts.get(contactIds.get(0)).getUserDescription());
    }


    @Test
    void testContact() {
        DBReader.Contact contact = dbReader.new Contact("a", "aa", 5.67);
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
        HashMap<Long, DBReader.Contact> cs = new HashMap<>();
        ArrayList<String> tweets = new ArrayList<>();
        for (Long id : cids) {
            cs.put(id, dbReader.new Contact("a"+id, "aa"+id, (10l-id)*(10l-id)));
            String tweet = "";
            for (int i = 0; i < id; ++i) {
                tweet += "cloud ";
            }
            cs.get(id).addTweet(tweet, "cloud");
            tweets.add(tweet);
        }
        ArrayList<DBReader.Contact> fcs = dbReader.filterContact(cids, cs, 3);
        assertEquals(tweets.get(2), fcs.get(0).getTweetText());
        assertEquals(tweets.get(1), fcs.get(1).getTweetText());
        assertEquals(tweets.get(3), fcs.get(2).getTweetText());
    }
}
