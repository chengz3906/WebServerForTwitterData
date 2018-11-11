package cmu.cc.team.spongebob.query1.qrcode;

import cmu.cc.team.spongebob.query1.qrcode.utils.BigEndianBitSet;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import org.apache.commons.lang3.StringUtils;


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

    private int[][] POS_DETECTION_PATTERN;
    private int[][] POS_DETECTION_MASKS;

    /**
     * sequences of coordinates that describes how to put a payload.
     */
    private final ArrayList<ArrayList<Integer>> QR_PAYLOAD_FILL_SEQ_V1;
    private final ArrayList<ArrayList<Integer>> QR_PAYLOAD_FILL_SEQ_V2;

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
    // private final byte[] QR_PAYLOAD_PAD2 = new byte[]{(byte) 0b11101100, (byte) 0b00010001};

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
        QR_PAYLOAD_FILL_SEQ_V1 = loadPayloadFillSeq(21);
        QR_PAYLOAD_FILL_SEQ_V2 = loadPayloadFillSeq(25);
        ENCODE_LOG_MAP_V1 = buildEncodeLogisticMap(21);
        // printQRCode(ENCODE_LOG_MAP_V1, 21);
        ENCODE_LOG_MAP_V2 = buildEncodeLogisticMap(25);
        // printQRCode(ENCODE_LOG_MAP_V2, 25);
        DECODE_LOG_MAP = buildEncodeLogisticMap(32);
        loadPositionDetectionPattern1();
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
            fillSeq = QR_PAYLOAD_FILL_SEQ_V1.get(0);
            logisticMap = ENCODE_LOG_MAP_V1;
        } else {
            qrCode = new int[20];
            qrCodeSize = 25;
            qrCodeTemplate = QR_V2_TEMPLATE;
            fillSeq = QR_PAYLOAD_FILL_SEQ_V2.get(0);
            logisticMap = ENCODE_LOG_MAP_V2;
        }

        // printQRCode(qrCodeTemplate, qrCodeSize);

        // put payload in qrCode
        // TODO use bitset here
        byte[] payload = messageToQRPayload(message);
        for (int i = 0; i < payload.length; i++) {
            for (int j = 0; j < 8; j++) {
                if (((payload[i] >> (7 - j)) & 1) == 1) {
                    int ind = i * 8 + j;
                    int intInd = fillSeq.get(ind) / 32;
                    int bitInd = fillSeq.get(ind) % 32;
                    qrCode[intInd] = qrCode[intInd] | (1 << (31 - bitInd));
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
                qrCode[intInd] = qrCode[intInd] | (1 << (31 - bitInd));
            }

            padBitInd++;
        }

        qrCode = integerArrayOr(qrCode, qrCodeTemplate);
        // printQRCode(qrCode, qrCodeSize);

        if (encrypt) {
            qrCode = integerArrayXor(qrCode, logisticMap);
        }

        qrCode[qrCode.length - 1] = qrCode[qrCode.length - 1] >>> (32 - (qrCodeSize * qrCodeSize % 32));

        // encode qrCode to hex string
        StringBuilder stringBuilder = new StringBuilder();
        for (int i : qrCode) {
            stringBuilder.append(String.format("0x%x", i).toLowerCase());
        }

        return stringBuilder.toString();
    }

    private static void printQRCode(int[] qrCode, int size) {
        System.out.println();
        for (int i = 0; i < qrCode.length; i++) {
            for (int j = 0; j < size; j++) {
                int intInd = (i * size + j) / 32;
                int bitInd = (i * size + j) % 32;

                if (((qrCode[intInd] >>> (size - 1 - bitInd)) & 1) == 1) {
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
        int[] encoded = hexStringToIntArray(encryptedHexString);
        int[] decoded = integerArrayXor(encoded, DECODE_LOG_MAP);
        // printQRCode(decoded, 32);

        int size = -1;
        int rStart = -1;
        int cStart = -1;
        int rot = -1;

        for (int i = 0; i < 32 - 21; i++) {
            for (int j = 0; j < 32 - 21; j++) {
                if (checkPosAlign(i, j, decoded)) { // find the top-left
                    for (int s: Arrays.asList(21, 25)) {
                        size = s;
                        if (checkPosAlign(i, j + size - 7, decoded)
                                && checkPosAlign(i + size - 7, j, decoded)) {
                            rot = 0;
                            break;
                        } else if (checkPosAlign(i, j + size - 7, decoded) && checkPosAlign(i + size - 7, j + size - 7, decoded)) {
                            rot = 1;
                            break;
                        } else if (checkPosAlign(i + size - 7, j, decoded) && checkPosAlign(i + size - 7, j + size - 7, decoded)) {
                            rot = 3;
                            break;
                        }
                    }

                    // not find
                    if (rot == -1) {
                        size = -1;
                    }
                } else if (checkPosAlign(i, j + 21 - 7, decoded)) {
                    // find the top-right in V1
                    if (checkPosAlign(i, j + 21 - 7, decoded) && checkPosAlign(i + 21 - 7, j + 21 - 7, decoded)) {
                        rot = 2;
                        size = 21;
                    }

                } else if (checkPosAlign(i, j + 25 - 7, decoded)) {
                    // find the top-right in V2
                    if (checkPosAlign(i, j + 25 - 7, decoded) && checkPosAlign(i + 25 - 7, j + 25 - 7, decoded)) {
                        rot = 2;
                        size = 25;
                    }
                }


                if (rot != -1) {
                    rStart = i;
                    cStart = j;
                    break;
                }
            }

            if (rot != -1) {
                break;
            }
        }

        if (size < 0 || rStart < 0 || cStart < 0 || rot < 0) {
            throw new QRParsingException();
        }

        String message = getQRPayload(decoded, rStart, cStart, rot, size);

        return message;
    }

    private boolean checkPosAlign(int rStart, int cStart, int[] searchSpace) {
        if (rStart + 7 > 31) return false;
        if (cStart + 7 > 31) return false;

        for (int i = 0; i < 7; i++) {
            int shiftedMask = this.POS_DETECTION_MASKS[cStart][i];
            int shiftedPosPat = this.POS_DETECTION_PATTERN[cStart][i];
            if (((shiftedMask & searchSpace[rStart + i]) ^ shiftedPosPat) != 0) {
                return false;
            }
        }
        return true;
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

    String getQRPayload(int[] searchSpace, int rStart, int cStart, int rot, int size) {
        int messageLen = 0;
        ArrayList<Integer> fillSeq;

        if (size == 21) {
            fillSeq = QR_PAYLOAD_FILL_SEQ_V1.get(rot);
        } else {
            fillSeq = QR_PAYLOAD_FILL_SEQ_V2.get(rot);
        }

        for (int i = 0; i < 8; i++) {
            int ind = fillSeq.get(i);
            int r = ind / size;
            int c = ind % size;

            int rind = (r + rStart) * 32 + cStart + c;
            int rr = rind / 32;
            int rc = rind % 32;

            if (((searchSpace[rr] >> (31 - rc)) & 1) == 1) {
                messageLen = (messageLen | (1 << (7 - i)));
            }
        }

        char[] messageBytes = new char[messageLen];
        for (byte i = 0; i < messageLen; i++) {
            for (int j = 0; j < 8; j++) {
                int ind = fillSeq.get(8 + 16 * i + j);
                int r = ind / size;
                int c = ind % size;

                int rind = (r + rStart) * 32 + cStart + c;
                int rr = rind / 32;
                int rc = rind % 32;

                if (((searchSpace[rr] >> (31 - rc)) & 1) == 1) {
                    messageBytes[i] = (char) (messageBytes[i] | (1 << (7 - j)));
                }
            }
        }

        return new String(messageBytes);
    }


    private int[] loadQRCodeTemplate(int size) {
        int[] template;

        ClassLoader classLoader = getClass().getClassLoader();
        InputStream in;
        if (size == 21) {
            in = classLoader.getResourceAsStream("QR_template_v1.txt");
            template = new int[14];
        } else {
            in = classLoader.getResourceAsStream("QR_template_v2.txt");
            template = new int[20];
        }

        try (Scanner scanner = new Scanner(in)) {
            int r = 0;
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                for (int c = 0; c < size; c++) {
                    if (line.charAt(c) == '1') {
                        int intInd = (r * size + c) / 32;
                        int bitInd = (r * size + c) % 32;
                        template[intInd] =  template[intInd] | (1 << (31 - bitInd));
                    }
                }
                r++;
            }
        }

        return template;
    }

    private void loadPositionDetectionPattern1() {
        int[] pattern = new int[]{0b1111111,
                0b1000001,
                0b1011101,
                0b1011101,
                0b1011101,
                0b1000001,
                0b1111111};
        int[] mask = new int[]{0b1111111, 0b1111111, 0b1111111, 0b1111111, 0b1111111, 0b1111111, 0b1111111};

        int[][] patterns = new int[32-7][];
        int[][] masks = new int[32-7][];

        for (int i = 0; i < 32 - 7; i++) {
            patterns[i] = new int[7];
            masks[i] = new int[7];
            for (int j = 0; j < 7; j++) {
                patterns[i][j] = pattern[j] << (25 - i);
                masks[i][j] = mask[j] << (25 - i);
            }
        }

        this.POS_DETECTION_PATTERN = patterns;
        this.POS_DETECTION_MASKS = masks;
    }

    private int[] hexStringToIntArray(String hexString) {
        String[] tokens = StringUtils.splitByWholeSeparator(hexString, "0x");
        int[] arr = new int[tokens.length];

        for (int i = 0; i < tokens.length; i++) {
            arr[i] =  (int) Long.parseLong(tokens[i], 16);
        }
        return arr;
    }

    private ArrayList<ArrayList<Integer>> loadPayloadFillSeq(int size) {
        ArrayList<ArrayList<Integer>> fillSeq = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            fillSeq.add(new ArrayList<>());
        }

        ClassLoader classLoader = getClass().getClassLoader();
        InputStream in;
        if (size == 21) {
            in = classLoader.getResourceAsStream("QR_fill_v1.txt");
        } else {
            in = classLoader.getResourceAsStream("QR_fill_v2.txt");
        }

        try (Scanner scanner = new Scanner(in)) {
            while (scanner.hasNextLine()) {
                String[] coord = StringUtils.split(scanner.nextLine(), ',');
                int r = Integer.parseInt(coord[0]);
                int c = Integer.parseInt(coord[1]);
                fillSeq.get(0).add(r * size + c);  // rotate 0

                for (int i = 1; i < 4; i++) {
                    int tmp = c;
                    c = size - r - 1;
                    r = tmp;
                    fillSeq.get(i).add(r * size + c);
                }
            }
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
            logisticMap[i] = Integer.reverse(logisticMapByteBuffer.getInt(i * 4));
        }

        return logisticMap; // bit-level little-endian
    }

    private byte errorCode(byte b) {
        return (byte) ((b & 1) ^ ((b >> 1) & 1) ^ ((b >> 2) & 1) ^ ((b >> 3) & 1)
                ^ ((b >> 4) & 1) ^ ((b >> 5) & 1) ^ ((b >> 6) & 1) ^ ((b >> 7) & 1));
    }

}
