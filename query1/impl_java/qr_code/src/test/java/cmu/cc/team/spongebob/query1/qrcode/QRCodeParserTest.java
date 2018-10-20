package cmu.cc.team.spongebob.query1.qrcode;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;


class QRCodeParserTest {
    @Test
    void testMessageToQRPayload() {
        QRCodeParser encoder = new QRCodeParser();

        String text = "Test";
        ArrayList<Byte> payload = encoder.messageToQRPayload(text);

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
    void testQRCodeEncode() {
        QRCodeParser encoder = new QRCodeParser();
        assertEquals(encoder.encode("CC Team", false), "0xfe03fc120xd06e82bb0x74b5dba70x2ec111070xfaafe00e0x8a05170x492f60x599912030x7d80003a0xff889100x4c00ba050x35d088ee0x964504120x1fca80");
        assertEquals(encoder.encode("CC Team is awesome!", false), "0xfe373fc10x38106e990x8bb740050xdba002ec0x10c907fa0xaafe00090xed8880x58d9a8c40x984117080xe20000x3240020b0xbc4140080xa8600ecc0xf80019440xff902b100x48918ba00xfd5d0070x6ee861410x42a22fe0x3800");
    }

    @Test
    void testQRCodeEncrypt() {
        QRCodeParser encoder = new QRCodeParser();
        assertEquals(encoder.encode("CC Team", true), "0x66d92b800x5bc76d830x121a7fa60x51c111870x3a5f3ca30x8be36a130xedb223a0xfc8e98780x33bf50de0x2e8709700x545a2d0f0xecef7ae0x461175cd0xff132a");
        assertEquals(encoder.encode("CC Team is awesome!", true), "0x66ede8530xb3b981a10xed18e4040xa4a0026c0xd039db570x21976f0d0xed168440xfdce22bf0xd67e47ec0x2171a0600x2a1a95010x875f3f480x78347f130x886ccc430xc90f439a0x331f54900x7bbcbf030x20d731250xc555223e0x15858");
    }
}
