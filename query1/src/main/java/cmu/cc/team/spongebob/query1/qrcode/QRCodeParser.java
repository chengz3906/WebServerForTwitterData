package cmu.cc.team.spongebob.query1.qrcode;

import cmu.cc.team.spongebob.query1.qrcode.utils.BigEndianBitSet;
import cmu.cc.team.spongebob.query1.qrcode.utils.BitSquare;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.ListIterator;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;


// TODO make it a singleton class
public class QRCodeParser {
    /**
     * QR code templates.
     */
    private final BitSquare QR_V1_TEMPLATE;
    private final BitSquare QR_V2_TEMPLATE;

    /**
     * sequences of coordinates that describes how to put a payload.
     */
    private final ArrayList<ImmutablePair<Integer, Integer>> ZIGZAG_V1;
    private final ArrayList<ImmutablePair<Integer, Integer>> ZIGZAG_V2;

    /**
     * logistic maps.
     */
    private final BitSquare ENCODE_LOG_MAP_V1;
    private final BitSquare ENCODE_LOG_MAP_V2;
    private final BitSquare DECODE_LOG_MAP;

    /**
     * padding for QR payload.
     */
    private final BitSet QR_PAYLOAD_PAD = BigEndianBitSet.valueOf(
            new byte[]{(byte) 0b11101100, (byte) 0b00010001});

    /**
     * Error code check fails.
     */
    public class QRParsingException extends Exception {}

    /**
     * Default constructor.
     */
    public QRCodeParser() {
        QR_V1_TEMPLATE = loadQRCodeTemplate(21);
        QR_V2_TEMPLATE = loadQRCodeTemplate(25);
        ZIGZAG_V1 = loadZigZagFill(21);
        ZIGZAG_V2 = loadZigZagFill(25);
        ENCODE_LOG_MAP_V1 = buildEncodeLogisticMap(21);
        ENCODE_LOG_MAP_V2 = buildEncodeLogisticMap(25);
        DECODE_LOG_MAP = buildEncodeLogisticMap(32);
    }

    /**
     * Encode a message to QR code hex string.
     * @param message message
     * @param encrypt whether to use logical map
     * @return an encoded QR code as a hex string
     */
    public String encode(String message, boolean encrypt) {
        BitSquare qrCode = messageToBinarySquare(message, encrypt);
        return qrCode.toString();
    }

    /**
     * Decode a QR code hex string to message.
     * @param encryptedHexString encrypted QR code hex string
     * @return message
     * @throws QRParsingException when error code check fails
     */
    public String decode(String encryptedHexString) throws QRParsingException {
        BitSquare searchSpace = hexStringToBinarySquare(encryptedHexString, 32);
        searchSpace.xor(DECODE_LOG_MAP); // decrypt

        BitSquare qrCode = searchSpace.locateAndSlice(QR_V1_TEMPLATE);
        if (qrCode == null) {
            qrCode = searchSpace.locateAndSlice(QR_V2_TEMPLATE);
        }

        if (qrCode == null) {
            throw new QRParsingException();
        }

        byte[] payload = getQRPayload(qrCode);
        return qrPayloadToMessage(payload);
    }

    BitSquare messageToBinarySquare(String message, boolean encrypt) {
        BitSquare bitSquare;

        if (message.length() <= 13) {
            bitSquare = new BitSquare(21);
            bitSquare.add(QR_V1_TEMPLATE);
        } else {
            bitSquare = new BitSquare(25);
            bitSquare.add(QR_V2_TEMPLATE);
        }

        byte[] payload = messageToQRPayload(message);
        putQRPayload(bitSquare, payload);

        if (encrypt) {
            if (bitSquare.getSize() == 21) {
                bitSquare.xor(ENCODE_LOG_MAP_V1);
            } else {
                bitSquare.xor(ENCODE_LOG_MAP_V2);
            }
        }

        return bitSquare;
    }

    BitSquare hexStringToBinarySquare(String hexString, int size) {
        return BitSquare.fromHexString(hexString, size);
    }

    byte[] messageToQRPayload(String message) {
        byte[] payload = new byte[message.length() * 2 + 1];

        // message length
        payload[0] = new Integer(message.length()).byteValue();

        for (int i = 0; i < message.length(); i++) {
            byte character = (byte) message.charAt(i);
            byte errorCode = errorCode(character);
            payload[2 * i + 1] = character;
            payload[2 * i + 2] = errorCode;
        }

        return payload;
    }

    String qrPayloadToMessage(byte[] qrPayload) throws QRParsingException {
        int strLen = qrPayload[0];

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < strLen; i++) {
            byte c = qrPayload[i * 2 + 1];
            sb.append((char) c);

            if (errorCode(c) != qrPayload[2 * i + 2]) {
                throw new QRParsingException();
            }
        }
        return sb.toString();
    }

    byte[] getQRPayload(BitSquare qrCode) {
        ArrayList<ImmutablePair<Integer, Integer>> zigZagFill =
                getZigzagFillBySize(qrCode.getSize());

        int payloadBitLength = readQRCodeMessageLength(qrCode) * 2 * 8 + 8;

        // retrieve payload bit by bit
        BitSet payloadBits = new BitSet(payloadBitLength);
        for (int i = 0; i < payloadBitLength; i++) {
            ImmutablePair<Integer, Integer> coord = zigZagFill.get(i);
            if (qrCode.getBit(coord.getLeft(), coord.getRight())) {
                payloadBits.set(i);
            }
        }

        // BigEndianBitSet.toByteArray does not include trailing 0's, so I have to
        // normalize the length of what toByteArray returns to the actual payload length in bytes
        byte[] payload = new byte[payloadBitLength / 8];
        byte[] bytes = BigEndianBitSet.toByteArray(payloadBits);
        System.arraycopy(bytes, 0, payload, 0, bytes.length);

       return payload;
    }

    private void putQRPayload(BitSquare template, byte[] payload) {
        BitSquare payloadQRCode;
        ArrayList<ImmutablePair<Integer, Integer>> zigzag;

        if (template.getSize() == 21) {
            payloadQRCode = new BitSquare(21);
            zigzag = ZIGZAG_V1;
        } else {
            payloadQRCode = new BitSquare(25);
            zigzag = ZIGZAG_V2;
        }

        BitSet payloadBits = BigEndianBitSet.valueOf(payload);

        // put the payload bit by bit
        int setBitInd = payloadBits.nextSetBit(0);
        while (setBitInd != -1) {
            ImmutablePair<Integer, Integer> coord = zigzag.get(setBitInd);
            payloadQRCode.setBit(coord.getLeft(), coord.getRight());
            setBitInd = payloadBits.nextSetBit(setBitInd + 1);
        }

        // keep writing the dummy sequence to fill-able space
        ListIterator<ImmutablePair<Integer, Integer>> coordsToFill =
                zigzag.listIterator(payload.length * 8);
        int i = 0;
        while (coordsToFill.hasNext()) {
            ImmutablePair<Integer, Integer> coord = coordsToFill.next();

            if (QR_PAYLOAD_PAD.get(i % 16)) {
                payloadQRCode.setBit(coord.getLeft(), coord.getRight());
            }

            i++;
        }

        template.add(payloadQRCode);
    }

    private BitSquare loadQRCodeTemplate(int size) {
        BitSquare template = new BitSquare(size);

        ClassLoader classLoader = getClass().getClassLoader();
        File file;
        if (size == 21) {
            file = new File(classLoader.getResource("QR_template_v1.txt").getFile());
        } else {
            file = new File(classLoader.getResource("QR_template_v2.txt").getFile());
        }

        try (Scanner scanner = new Scanner(file)) {
            int row = 0;
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                for (int i = 0; i < size; i++) {
                    char c = line.charAt(i);
                    if (c == '1') {
                        template.setBit(row, i);
                    }
                }
                row++;
            }
        } catch (FileNotFoundException e) {
            System.out.println("fail to load QR template");
            e.printStackTrace();
        }

        return template;
    }

    private ArrayList<ImmutablePair<Integer, Integer>> loadZigZagFill(int size) {
        ArrayList<ImmutablePair<Integer, Integer>> zigzag = new ArrayList<>();

        ClassLoader classLoader = getClass().getClassLoader();
        File file;
        if (size == 21) {
            file = new File(classLoader.getResource("QR_fill_v1.txt").getFile());
        } else {
            file = new File(classLoader.getResource("QR_fill_v2.txt").getFile());
        }

        try (Scanner scanner = new Scanner(file)) {
            while (scanner.hasNextLine()) {
                String[] coord = StringUtils.split(scanner.nextLine(), ',');
                zigzag.add(new ImmutablePair<>(Integer.parseInt(coord[0]),
                        Integer.parseInt(coord[1])));
            }
        } catch (FileNotFoundException e) {
            System.out.println("fail to load zigzag");
            e.printStackTrace();
        }

        return zigzag;
    }

    private BitSquare buildEncodeLogisticMap(int size) {
        int logisticMapSize = (size * size) / 8 + 1;
        byte[] logisticMap = new byte[logisticMapSize];

        double xi = 0.1;
        double r = 4;
        logisticMap[0] = (byte) Math.floor(xi * 255);

        for (int i = 1; i < logisticMap.length; i++) {
            xi = (r * xi) * (1 - xi);
            logisticMap[i] = (byte) Math.floor(xi * 255);
        }

        return new BitSquare(size, BitSet.valueOf(logisticMap)); // bit-level little-endian
    }

    private BitSquare getQRTemplateBySize(int size) {
        if (size == 21) {
            return QR_V1_TEMPLATE;
        }

        return QR_V2_TEMPLATE;
    }

    private ArrayList<ImmutablePair<Integer, Integer>> getZigzagFillBySize(int size) {
        if (size == 21) {
            return ZIGZAG_V1;
        }

        return ZIGZAG_V2;
    }

    private byte errorCode(byte b) {
        return (byte) (Integer.bitCount((int) b) % 2);
    }

    private int readQRCodeMessageLength(BitSquare qrCode) {
        ArrayList<ImmutablePair<Integer, Integer>> zigZagFill =
                getZigzagFillBySize(qrCode.getSize());

        BitSet bits = new BitSet(8);
        for (int i = 0; i < 8; i++) {
            int r = zigZagFill.get(i).getLeft();
            int c = zigZagFill.get(i).getRight();

            if (qrCode.getBit(r, c)) {
                bits.set(i);
            }
        }

        return (int) BigEndianBitSet.toByte(bits);
    }
}
