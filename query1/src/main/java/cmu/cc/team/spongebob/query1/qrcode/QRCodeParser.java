package cmu.cc.team.spongebob.query1.qrcode;

import cmu.cc.team.spongebob.query1.qrcode.utils.BigEndianBitSet;
import cmu.cc.team.spongebob.query1.qrcode.utils.BitSquare;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;


// TODO make it a singleton class
public class QRCodeParser {
    /**
     * QR code templates.
     */
    private final int[] QR_V1_TEMPLATE;
    private final int[] QR_V2_TEMPLATE;

    /**
     * position detection patter
     */
    private final BitSquare POS_DETECTION_PATTERN;

    /**
     * sequences of coordinates that describes how to put a payload.
     */
    private final ArrayList<Integer> QR_PAYLOAD_FILL_SEQ_V1;
    private final ArrayList<Integer> QR_PAYLOAD_FILL_SEQ_V2;

    /**
     * logistic maps.
     */
    private final int[] ENCODE_LOG_MAP_V1;
    private final int[] ENCODE_LOG_MAP_V2;
    private final int[] DECODE_LOG_MAP;

    /**
     * padding for QR payload.
     */
    // TODO change this to an integer
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
        POS_DETECTION_PATTERN = loadPositionDetectionPattern();
        QR_PAYLOAD_FILL_SEQ_V1 = loadPayloadFillSeq(21);
        QR_PAYLOAD_FILL_SEQ_V2 = loadPayloadFillSeq(25);
        ENCODE_LOG_MAP_V1 = buildEncodeLogisticMap(21);
        // printQRCode(ENCODE_LOG_MAP_V1, 21);
        ENCODE_LOG_MAP_V2 = buildEncodeLogisticMap(25);
        // printQRCode(ENCODE_LOG_MAP_V2, 25);
        DECODE_LOG_MAP = buildEncodeLogisticMap(32);
    }

    /**
     * Encode a message to QR code hex string.
     * @param message message
     * @param encrypt whether to use logical map
     * @return an encoded QR code as a hex string
     */
    public String encode(String message, boolean encrypt) {
        int[] qrCode;
        int[] qrCodeTemplate;
        int[] logisticMap;
        int qrCodeSize;
        ArrayList<Integer> fillSeq;
        if (message.length() < 14) {
            qrCode = new int[14];
            qrCodeSize = 21;
            qrCodeTemplate = QR_V1_TEMPLATE;
            fillSeq = QR_PAYLOAD_FILL_SEQ_V1;
            logisticMap = ENCODE_LOG_MAP_V1;
        } else {
            qrCode = new int[20];
            qrCodeSize = 25;
            qrCodeTemplate = QR_V2_TEMPLATE;
            fillSeq = QR_PAYLOAD_FILL_SEQ_V2;
            logisticMap = ENCODE_LOG_MAP_V2;
        }

        // printQRCode(qrCodeTemplate, qrCodeSize);

        // put payload in qrCode
        byte[] payload = messageToQRPayload(message);
        for (int i = 0; i < payload.length; i++) {
            for (int j = 0; j < 8; j++) {
                if (((payload[i] >> (7 - j)) & 1) == 1) {
                    int intInd = fillSeq.get(i * 8 + j) / 32;
                    int bitInd = fillSeq.get(i * 8 + j) % 32;
                    qrCode[intInd] = qrCode[intInd] | (1 << bitInd);
                }
            }
        }

        // printQRCode(qrCode, qrCodeSize);

        // keep writing the dummy sequence to fill-able space
        ListIterator<Integer> indsToFill = fillSeq.listIterator(payload.length * 8);
        int padBitInd = 0;
        while (indsToFill.hasNext()) {
            int indToFill = indsToFill.next();

            if (QR_PAYLOAD_PAD.get(padBitInd % 16)) {
                int intInd = indToFill / 32;
                int bitInd = indToFill % 32;
                qrCode[intInd] = qrCode[intInd] | (1 << bitInd);
            }
            padBitInd++;
        }

        qrCode = integerArrayOr(qrCode, qrCodeTemplate);
        // printQRCode(qrCode, qrCodeSize);

        if (encrypt) {
            qrCode = integerArrayXor(qrCode, logisticMap);
        }

        qrCode[qrCode.length - 1] = qrCode[qrCode.length - 1] << (32 - (qrCodeSize * qrCodeSize % 32));

        // encode qrCode to hex string
        StringBuilder stringBuilder = new StringBuilder();
        for (Integer i : qrCode) {
            stringBuilder.append(String.format("0x%x", Integer.reverse(i)).toLowerCase());
        }

        return stringBuilder.toString();
    }

    private static void printQRCode(int[] qrCode, int size) {
        System.out.println();
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                int intInd = (i * size + j) / 32;
                int bitInd = (i * size + j) % 32;

                if (((qrCode[intInd] >>> bitInd) & 1) == 1) {
                    System.out.print("1 ");
                } else {
                    System.out.print("0 ");
                }
            }
            System.out.println();
        }
        System.out.println();
    }

    /**
     * Decode a QR code hex string to message.
     * @param encryptedHexString encrypted QR code hex string
     * @return message
     * @throws QRParsingException when error code check fails
     */
    public String decode(String encryptedHexString) throws QRParsingException {
        return null;
    }

    private static int[] integerArrayOr(int[] arr1, int[] arr2) {
        int[] or = new int[arr1.length];
        for (int i = 0; i < arr1.length; i++) {
            or[i] = arr1[i] | arr2[i];
        }
        return or;
    }

    private static int[] integerArrayAnd(int[] arr1, int[] arr2) {
        int[] and = new int[arr1.length];
        for (int i = 0; i < arr1.length; i++) {
            and[i] = arr1[i] & arr2[i];
        }
        return and;
    }

    private static int[] integerArrayXor(int[] arr1, int[] arr2) {
        int[] xor = new int[arr1.length];
        for (int i = 0; i < arr1.length; i++) {
            xor[i] = arr1[i] ^ arr2[i];
        }
        return xor;
    }


    BitSquare hexStringToBinarySquare(String hexString, int size) {
        return BitSquare.fromHexString(hexString, size);
    }

    byte[] messageToQRPayload(String message) {
        byte[] payload = new byte[message.length() * 2 + 1];

        // message length
        payload[0] = (byte) message.length();

        for (int i = 0; i < message.length(); i++) {
            byte character = (byte) message.charAt(i);
            byte errorCode = errorCode(character);
            payload[2 * i + 1] = (byte) message.charAt(i);
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

    /*
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
    */


    private int[] loadQRCodeTemplate(int size) {
        int[] template;

        ClassLoader classLoader = getClass().getClassLoader();
        File file;
        if (size == 21) {
            file = new File(Objects.
                    requireNonNull(classLoader.getResource("QR_template_v1.txt")).getFile());
            template = new int[14];
        } else {
            file = new File(Objects
                    .requireNonNull(classLoader.getResource("QR_template_v2.txt")).getFile());
            template = new int[20];
        }

        try (Scanner scanner = new Scanner(file)) {
            int r = 0;
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                for (int c = 0; c < size; c++) {
                    if (line.charAt(c) == '1') {
                        int intInd = (r * size + c) / 32;
                        int bitInd = (r * size + c) % 32;
                        template[intInd] =  template[intInd] | (1 << bitInd);
                    }
                }
                r++;
            }
        } catch (FileNotFoundException e) {
            System.out.println("fail to load QR template");
            e.printStackTrace();
        }

        return template;
    }

    private BitSquare loadPositionDetectionPattern() {
        BitSquare posDectionPattern = new BitSquare(7);

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("QR_position_detection_pattern.txt").getFile());

        try (Scanner scanner = new Scanner(file)) {
            int row = 0;
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                for (int i = 0; i < 7; i++) {
                    char c = line.charAt(i);
                    if (c == '1') {
                        posDectionPattern.setBit(row, i);
                    }
                }
                row++;
            }
        } catch (FileNotFoundException e) {
            System.out.println("fail to load QR Position Detection Pattern");
            e.printStackTrace();
        }

        return posDectionPattern;
    }


    private ArrayList<Integer> loadPayloadFillSeq(int size) {
        ArrayList<Integer> fillSeq = new ArrayList<>();

        ClassLoader classLoader = getClass().getClassLoader();
        File file;
        if (size == 21) {
            file = new File(Objects
                    .requireNonNull(classLoader.getResource("QR_fill_v1.txt")).getFile());
        } else {
            file = new File(Objects
                    .requireNonNull(classLoader.getResource("QR_fill_v2.txt")).getFile());
        }

        try (Scanner scanner = new Scanner(file)) {
            while (scanner.hasNextLine()) {
                String[] coord = StringUtils.split(scanner.nextLine(), ',');
                int r = Integer.parseInt(coord[0]);
                int c = Integer.parseInt(coord[1]);
                fillSeq.add(r * size + c);
            }
        } catch (FileNotFoundException e) {
            System.out.println("fail to load payload fill sequence");
            e.printStackTrace();
        }

        return fillSeq;
    }

    private int[] buildEncodeLogisticMap(int size) {
        int integerLen = (int) Math.ceil((size * size) / 32.0);
        ByteBuffer logisticMapByteBuffer = ByteBuffer.allocate(integerLen * 4);

        double xi = 0.1;
        double r = 4;
        logisticMapByteBuffer.put((byte) Math.floor(xi * 255));

        for (int i = 1; i < integerLen * 4; i++) {
            xi = (r * xi) * (1 - xi);
            logisticMapByteBuffer.put(i, (byte) Math.floor(xi * 255));
        }

        logisticMapByteBuffer.order(ByteOrder.LITTLE_ENDIAN);

        int[] logisticMap = new int[integerLen];

        for (int i = 0; i < integerLen; i++) {
            logisticMap[i] = logisticMapByteBuffer.getInt(i * 4);
        }

        // logisticMap[integerLen - 1] = logisticMap[integerLen - 1] >>> (32 - (size * size % 32));

        return logisticMap; // bit-level little-endian
    }

    private byte errorCode(byte b) {
        return (byte) (Integer.bitCount((int) b) % 2);
    }

}
