package cmu.cc.team.spongebob.query1.qrcode;

import cmu.cc.team.spongebob.query1.qrcode.utils.BinaryMap;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;


class QRCodeParserTest {

    private static final String CC_TEAM_QR_CODE = "0xfe03fc120xd06e82bb0x74b5dba70x2ec111070xfaafe00e0x8a05170x492f60x599912030x7d80003a0xff889100x4c00ba050x35d088ee0x964504120x1fca80";
    public static final String CC_TEAM_IS_AWESOME_QR_CODE = "0xfe373fc10x38106e990x8bb740050xdba002ec0x10c907fa0xaafe00090xed8880x58d9a8c40x984117080xe20000x3240020b0xbc4140080xa8600ecc0xf80019440xff902b100x48918ba00xfd5d0070x6ee861410x42a22fe0x3800";
    public static final String CC_TEAM_QR_CODE_ENCRYPT = "0x66d92b800x5bc76d830x121a7fa60x51c111870x3a5f3ca30x8be36a130xedb223a0xfc8e98780x33bf50de0x2e8709700x545a2d0f0xecef7ae0x461175cd0xff132a";
    public static final String CC_TEAM_IS_AWESOME_QR_CODE_ENCRYPT = "0x66ede8530xb3b981a10xed18e4040xa4a0026c0xd039db570x21976f0d0xed168440xfdce22bf0xd67e47ec0x2171a0600x2a1a95010x875f3f480x78347f130x886ccc430xc90f439a0x331f54900x7bbcbf030x20d731250xc555223e0x15858";

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
        assertEquals(encoder.encodeToString("CC Team", false), CC_TEAM_QR_CODE);
        assertEquals(encoder.encodeToString("CC Team is awesome!", false), CC_TEAM_IS_AWESOME_QR_CODE);
    }

    @Test
    void testQRCodeEncrypt() {
        QRCodeParser encoder = new QRCodeParser();
        assertEquals(encoder.encodeToString("CC Team", true), CC_TEAM_QR_CODE_ENCRYPT);
        assertEquals(encoder.encodeToString("CC Team is awesome!", true), CC_TEAM_IS_AWESOME_QR_CODE_ENCRYPT);
    }

    @Test
    void testHexStringToBinaryMap() {
        QRCodeParser encoder = new QRCodeParser();
        BinaryMap decoded = encoder.decodeToBinaryMap(CC_TEAM_QR_CODE, 21);
        BinaryMap expected = encoder.encodeToBinaryMap("CC Team", false);
        assertEquals(decoded.toStringBinaryMap(), expected.toStringBinaryMap());

        BinaryMap decoded2 = encoder.decodeToBinaryMap(CC_TEAM_IS_AWESOME_QR_CODE, 25);
        BinaryMap expected2 = encoder.encodeToBinaryMap("CC Team is awesome!", false);
        assertEquals(decoded2.toStringBinaryMap(), expected2.toStringBinaryMap());
    }

    @Test
    void testBinaryMapRotation() {
        QRCodeParser encoder = new QRCodeParser();
        BinaryMap decoded = encoder.decodeToBinaryMap(CC_TEAM_QR_CODE, 21);

        printDivider("rotate 90");
        decoded.rotate(90);
        System.out.println(decoded.toStringBinaryMap());

        printDivider("rotate another 180");
        decoded.rotate(180);
        System.out.println(decoded.toStringBinaryMap());

        printDivider("rotate another 90");
        decoded.rotate(90);
        System.out.println(decoded.toStringBinaryMap());

        BinaryMap expected = encoder.encodeToBinaryMap("CC Team", false);
        assertEquals(decoded.toStringBinaryMap(), expected.toStringBinaryMap());
    }

    private void printDivider(String text) {
        System.out.print(StringUtils.repeat('-', 5));
        System.out.print(text);
        System.out.print(StringUtils.repeat('-', 5));
        System.out.print('\n');
    }
}
