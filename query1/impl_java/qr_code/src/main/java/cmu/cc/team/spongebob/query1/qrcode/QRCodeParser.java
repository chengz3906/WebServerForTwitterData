package cmu.cc.team.spongebob.query1.qrcode;

import cmu.cc.team.spongebob.query1.qrcode.utils.BigEndianBitSet;
import cmu.cc.team.spongebob.query1.qrcode.utils.BinaryMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.ListIterator;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;


public class QRCodeParser {
    /**
     * QR code templates.
     */
    private final BinaryMap QR_V1_TEMPLATE;
    private final BinaryMap QR_V2_TEMPLATE;

    /**
     * sequences of coordinates that describes how to put a payload.
     */
    private final ArrayList<ImmutablePair<Integer, Integer>> ZIGZAG_V1;
    private final ArrayList<ImmutablePair<Integer, Integer>> ZIGZAG_V2;

    /**
     * logistic maps.
     */
    private final BinaryMap ENCODE_LOG_MAP_V1;
    private final BinaryMap ENCODE_LOG_MAP_V2;
    private final BinaryMap DECODE_LOG_MAP;

    /**
     * padding for QR payload.
     */
    private final BitSet QR_PAYLOAD_PAD = BigEndianBitSet.valueOf(
            new byte[]{(byte) 0b11101100, (byte) 0b00010001});

    public QRCodeParser() {
        QR_V1_TEMPLATE = loadQRCodeTemplate(21);
        QR_V2_TEMPLATE = loadQRCodeTemplate(25);
        ZIGZAG_V1 = loadZigZagFill(21);
        ZIGZAG_V2 = loadZigZagFill(25);
        ENCODE_LOG_MAP_V1 = buildEncodeLogisticMap(21);
        ENCODE_LOG_MAP_V2 = buildEncodeLogisticMap(25);
        DECODE_LOG_MAP = buildEncodeLogisticMap(32);
    }

    public String encodeToString(String message, boolean encrypt) {
        BinaryMap qrCode = encodeToBinaryMap(message, encrypt);

        return qrCode.toString();
    }


    public String decodeToString(String qrCode) {
        return "";
    }

    BinaryMap encodeToBinaryMap(String message, boolean encrypt) {
        BinaryMap qrCode;

        if (message.length() <= 13) {
            qrCode = new BinaryMap(21);
            qrCode.add(QR_V1_TEMPLATE);
        } else {
            qrCode = new BinaryMap(25);
            qrCode.add(QR_V2_TEMPLATE);
        }

        ArrayList<Byte> payload = messageToQRPayload(message);
        putPayload(qrCode, payload);

        if (encrypt) {
            if (qrCode.getSize() == 21) {
                qrCode.xor(ENCODE_LOG_MAP_V1);
            } else {
                qrCode.xor(ENCODE_LOG_MAP_V2);
            }
        }

        return qrCode;
    }

    BinaryMap decodeToBinaryMap(String message, int size) {
        BinaryMap qrCode = BinaryMap.valueOf(message, size);
        qrCode.print();

        return qrCode;
    }

    ArrayList<Byte> messageToQRPayload(String message) {
        ArrayList<Byte> payload = new ArrayList<>();

        // message length
        payload.add((new Integer(message.length()).byteValue()));

        for (int i = 0; i < message.length(); i++) {
            byte character = (byte) message.charAt(i);
            byte errorCode = errorCode(character);
            payload.add(character);
            payload.add(errorCode);
        }

        return payload;
    }

    private void putPayload(BinaryMap template, ArrayList<Byte> payload) {
        BinaryMap payloadQRCode;
        ArrayList<ImmutablePair<Integer, Integer>> zigzag;

        if (template.getSize() == 21) {
            payloadQRCode = new BinaryMap(21);
            zigzag = ZIGZAG_V1;
        } else {
            payloadQRCode = new BinaryMap(25);
            zigzag = ZIGZAG_V2;
        }

        BitSet payloadBits = BigEndianBitSet.valueOf(payload);

        // put the payload
        int setBitInd = payloadBits.nextSetBit(0);
        while (setBitInd != -1) {
            ImmutablePair<Integer, Integer> coord = zigzag.get(setBitInd);
            payloadQRCode.setBit(coord.getLeft(), coord.getRight());
            setBitInd = payloadBits.nextSetBit(setBitInd + 1);
        }

        ListIterator<ImmutablePair<Integer, Integer>> coordsToFill =
                zigzag.listIterator(payload.size() * 8);

        // keep writing the sequence of 11101100 00010001
        int i = 0;
        while (coordsToFill.hasNext()) {
            ImmutablePair<Integer, Integer> coord = coordsToFill.next();

            if (QR_PAYLOAD_PAD.get(i % 16)) {
                payloadQRCode.setBit(coord.getLeft(), coord.getRight());
            }

            i++;
        }

        template.add(payloadQRCode);

        // TODO remove after test
        // template.print();
    }

    private BinaryMap loadQRCodeTemplate(int size) {
        BinaryMap template = new BinaryMap(size);

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

    private BinaryMap buildEncodeLogisticMap(int size) {
        int logisticMapSize = (size * size) / 8 + 1;
        byte[] logisticMap = new byte[logisticMapSize];

        double xi = 0.1, r = 4;
        logisticMap[0] = (byte) Math.floor(xi * 255);

        for (int i = 1; i < logisticMap.length; i++) {
            xi = (r * xi) * (1 - xi);
            logisticMap[i] = (byte) Math.floor(xi * 255);
        }

        return new BinaryMap(size, BitSet.valueOf(logisticMap)); // bit-level little-endian
    }

    private byte errorCode(byte b) {
        return (byte) (Integer.bitCount((int) b) % 2);
    }
}
