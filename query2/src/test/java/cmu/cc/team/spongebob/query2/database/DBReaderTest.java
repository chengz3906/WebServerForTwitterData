package cmu.cc.team.spongebob.query2.database;

import org.junit.jupiter.api.Test;
import cmu.cc.team.spongebob.query2.database.DBReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.ArrayList;
import java.util.Arrays;

public class DBReaderTest {

    private final Long userId = 2430760566l;

    @Test
    void testGetContacts() {
        DBReader dbReader = new DBReader();
        ArrayList<Long> contactUid = new ArrayList<>();
        ArrayList<Long> contactTid = new ArrayList<>();
        ArrayList<Long> expectedUid = new ArrayList<Long>
                (Arrays.asList(2435016596l, 2435832774l, 2443871671l, 2423836764l));
        ArrayList<Long> expectedTid = new ArrayList<Long>
                (Arrays.asList(457157292069511169l, 471976254644973568l,
                        462125453546119168l, 473368121857044480l));
        dbReader.getContacts(userId, contactTid, contactUid);
        for (int i = 0; i < contactTid.size(); ++i) {
            assertEquals(contactTid.get(i), expectedTid.get(i));
            assertEquals(contactUid.get(i), expectedUid.get(i));
        }
    }


}
