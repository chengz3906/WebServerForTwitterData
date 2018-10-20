package cmu.cc.team.spongebob.query1.qrcode;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static cmu.cc.team.spongebob.query1.qrcode.QRCode.buildQRCodeTemplate;
import static cmu.cc.team.spongebob.query1.qrcode.QRCode.messageToQRPayload;
import static org.junit.jupiter.api.Assertions.assertEquals;


class QRCodeTest {
    @Test
    void testMessageToQRPayload() {
        String text = "Test";
        ArrayList<Byte> payload = messageToQRPayload(text);

        ArrayList<Byte> expected = new ArrayList<>();

        expected.add((byte) 0b100);
        expected.add((byte) 0b1010100);
        expected.add((byte) 0b1);
        expected.add((byte) 0b1100101);
        expected.add((byte) 0b0);
        expected.add((byte) 0b1110011);
        expected.add((byte) 0b1);
        expected.add((byte) 0b1110100);
        expected.add((byte) 0b0);

        assertEquals(expected, payload);
    }

    @Test
    void testQRCodeTemplate() {
        char[][] templateV1 = buildQRCodeTemplate(21);
        System.out.println("21x21 QR template");
        for (int i = 0; i < 21; i++) {
            System.out.println(templateV1[i]);
        }

        char[][] templateV2 = buildQRCodeTemplate(25);
        System.out.println("25x25 QR template");
        for (int i = 0; i < 25; i++) {
            System.out.println(templateV2[i]);
        }
    }
}
