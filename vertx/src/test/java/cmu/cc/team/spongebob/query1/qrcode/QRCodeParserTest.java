package cmu.cc.team.spongebob.query1.qrcode;

import cmu.cc.team.spongebob.query1.qrcode.utils.BitSquare;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


class QRCodeParserTest {

    private static final String CC_TEAM_QR_CODE = "0xfe03fc120xd06e82bb0x74b5dba70x2ec111070xfaafe00e0x8a05170x492f60x599912030x7d80003a0xff889100x4c00ba050x35d088ee0x964504120x1fca80";
    private static final String CC_TEAM_IS_AWESOME_QR_CODE = "0xfe373fc10x38106e990x8bb740050xdba002ec0x10c907fa0xaafe00090xed8880x58d9a8c40x984117080xe20000x3240020b0xbc4140080xa8600ecc0xf80019440xff902b100x48918ba00xfd5d0070x6ee861410x42a22fe0x3800";
    private static final String CC_TEAM_QR_CODE_ENCRYPT = "0x66d92b800x5bc76d830x121a7fa60x51c111870x3a5f3ca30x8be36a130xedb223a0xfc8e98780x33bf50de0x2e8709700x545a2d0f0xecef7ae0x461175cd0xff132a";
    private static final String CC_TEAM_IS_AWESOME_QR_CODE_ENCRYPT = "0x66ede8530xb3b981a10xed18e4040xa4a0026c0xd039db570x21976f0d0xed168440xfdce22bf0xd67e47ec0x2171a0600x2a1a95010x875f3f480x78347f130x886ccc430xc90f439a0x331f54900x7bbcbf030x20d731250xc555223e0x15858";
    private static final String CC_TEAM_IS_AWESOME_QR_EMBED = "0x2b23d6830x15a0de0d0x744784010x29e880700xfe1adf5c0xb96061290x1127b67c0x311690430xc63153140xf6e00650x92d3960b0xf59a79070x704e73d40x977fd8090xf516e98a0x3e0c19f10xac626d040x6a3e58650xca85aa3e0x6266b640x842ddcb40x4e7c879c0x85dd21240x3afae3dc0xe07908a70x664685970xb38246f70x511908330x40a111ee0xc12c8fd10x82984c520x4ddee6f6";
    private static final QRCodeParser QR_CODE_PARSER = new QRCodeParser();

    @Test
    void testMessageToQRPayload() {
        QRCodeParser encoder = new QRCodeParser();

        String text = "Test";
        byte[] payload = encoder.messageToQRPayload(text);

        byte[] expected = new byte[] {
                (byte) 0b100,
                (byte) 0b1010100,
                (byte) 0b1,
                (byte) 0b1100101,
                (byte) 0b0,
                (byte) 0b1110011,
                (byte) 0b1,
                (byte) 0b1110100,
                (byte) 0b0
        };

        assertArrayEquals(expected, payload);
    }

    @Test
    void testQRPayloadToMessage() {
        byte[] payload = QR_CODE_PARSER.messageToQRPayload("Test");
        String decoded = null;
        try {
            decoded = QR_CODE_PARSER.qrPayloadToMessage(payload);
        } catch (QRCodeParser.QRParsingException e) {
            e.printStackTrace();
            fail();
        }
        assertEquals("Test", decoded);
    }

    @Test
    void testQRCodeEncode() {
        assertEquals(QR_CODE_PARSER.encode("CC Team", false), CC_TEAM_QR_CODE);
        assertEquals(QR_CODE_PARSER.encode("CC Team is awesome!", false), CC_TEAM_IS_AWESOME_QR_CODE);
    }

    @Test
    void testQRCodeDecode() {
        String decoded = "";
        try {
            decoded = QR_CODE_PARSER.decode(CC_TEAM_IS_AWESOME_QR_EMBED);
        } catch (QRCodeParser.QRParsingException e) {
            e.printStackTrace();
            fail();
        }

        assertEquals("CC Team is awesome!", decoded);
    }

    @Test
    void testQRCodeEncrypt() {
        assertEquals(QR_CODE_PARSER.encode("CC Team", true), CC_TEAM_QR_CODE_ENCRYPT);
        assertEquals(QR_CODE_PARSER.encode("CC Team is awesome!", true), CC_TEAM_IS_AWESOME_QR_CODE_ENCRYPT);
    }

    @Test
    void testEncodeSpeed() {
        // TODO implement speed test
    }

    @Test
    void testDecodeSpeed() {
        // TODO implement speed test
    }

    @Test
    void testHexStringToBitSquare() {
        BitSquare decoded = QR_CODE_PARSER.hexStringToBinarySquare(CC_TEAM_QR_CODE, 21);
        BitSquare expected = QR_CODE_PARSER.messageToBinarySquare("CC Team", false);
        assertEquals(decoded.toStringPretty(), expected.toStringPretty());

        BitSquare decoded2 = QR_CODE_PARSER.hexStringToBinarySquare(CC_TEAM_IS_AWESOME_QR_CODE, 25);
        BitSquare expected2 = QR_CODE_PARSER.messageToBinarySquare("CC Team is awesome!", false);
        assertEquals(decoded2.toStringPretty(), expected2.toStringPretty());
    }

    @Test
    void testBitSquareToQRPayload() {
        BitSquare qrCode = QR_CODE_PARSER.messageToBinarySquare("CC Team", false);
        qrCode.print();

        byte[] expectedPayload = QR_CODE_PARSER.messageToQRPayload("CC Team");
        byte[] retrieved = QR_CODE_PARSER.getQRPayload(qrCode);

        assertArrayEquals(retrieved, expectedPayload);

        qrCode = QR_CODE_PARSER.messageToBinarySquare("CC Team is awesome!", false);
        expectedPayload = QR_CODE_PARSER.messageToQRPayload("CC Team is awesome!");
        retrieved = QR_CODE_PARSER.getQRPayload(qrCode);

        assertArrayEquals(expectedPayload, retrieved);
    }
}
