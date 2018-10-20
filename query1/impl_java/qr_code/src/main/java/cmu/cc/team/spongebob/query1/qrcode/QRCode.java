package cmu.cc.team.spongebob.query1.qrcode;

import java.util.ArrayList;


public class QRCode {
    /**
     * Pixels in QR code template.
     */
    private static final char PIXEL_ZERO = '0';
    private static final char PIXEL_ONE = '1';
    private static final char PIXEL_UNFILLED = 'x';
    private static final char PIXEL_BOUNDARY = 'b';

    private static final char[][] QR_POS_DECTION_PATTERN = new char[][]
            {
                    {'1', '1', '1', '1', '1', '1', '1'},
                    {'1', '0', '0', '0', '0', '0', '1'},
                    {'1', '0', '1', '1', '1', '0', '1'},
                    {'1', '0', '1', '1', '1', '0', '1'},
                    {'1', '0', '1', '1', '1', '0', '1'},
                    {'1', '0', '0', '0', '0', '0', '1'},
                    {'1', '1', '1', '1', '1', '1', '1'}
            };

    private static final char[][] QR_ALIGN_PATTERN = new char[][]
            {
                    {'1', '1', '1', '1', '1'},
                    {'1', '0', '0', '0', '1'},
                    {'1', '0', '1', '0', '1'},
                    {'1', '0', '0', '0', '1'},
                    {'1', '1', '1', '1', '1'}
            };

    /**
     * QR code templates.
     */
    private static final char[][] QR_V1_TEMPLATE = buildQRCodeTemplate(21);
    private static final char[][] QR_V2_TEMPLATE = buildQRCodeTemplate(25);

    public static String encode(String text) {
        return "";
    }

    static ArrayList<Byte> messageToQRPayload(String message) {
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

    private static byte errorCode(byte b) {
        // TODO find a faster method
        return (byte) (Integer.bitCount((int) b) % 2);
    }

    static char[][] buildQRCodeTemplate(int size) {
        char[][] template = new char[size][size];

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                template[i][j] = PIXEL_UNFILLED;
            }
        }

        // Position detection patterns
        fillPositionDetectionPattern(template, 0, 0);
        fillPositionDetectionPattern(template, 0, size - 7);
        fillPositionDetectionPattern(template, size - 7, 0);

        fillAlignPattern(template);
        fillTimingPattern(template);

        return template;
    }

    private static void embedPayload(ArrayList<Byte> payload) {

    }

    private static void fillPositionDetectionPattern(char[][] qrCode,
                                                     int rowOffset, int colOffset) {
        if (rowOffset > 0) {
            for (int i = 0; i < 8; i++) {
                qrCode[rowOffset - 1][i] = PIXEL_BOUNDARY;
            }
        } else {
            if (colOffset > 0) {
                for (int i = 0; i < 8; i++) {
                    qrCode[rowOffset + 7][colOffset + i - 1] = PIXEL_BOUNDARY;
                }
            } else {
                for (int i = 0; i < 8; i++) {
                    qrCode[rowOffset + 7][i] = PIXEL_BOUNDARY;
                }
            }
        }

        for (int i = 0; i < 7; i++) {
            if (colOffset > 0) {
                qrCode[rowOffset + i][colOffset - 1] = PIXEL_BOUNDARY;
            } else {
                qrCode[rowOffset + i][colOffset + 7] = PIXEL_BOUNDARY;
            }
            System.arraycopy(QR_POS_DECTION_PATTERN[i], 0, qrCode[rowOffset + i], colOffset, 7);
        }
    }

    private static void fillTimingPattern(char[][] qrCode) {
        for (int i = 6; i < qrCode.length; i++) {
            if (qrCode[i][6] == PIXEL_UNFILLED) {
                if (i % 2 == 0) {
                    qrCode[i][6] = PIXEL_ONE;
                } else {
                    qrCode[i][6] = PIXEL_ZERO;
                }
            }
        }

        for (int i = 6; i < qrCode.length; i++) {
            if (qrCode[6][i] == PIXEL_UNFILLED) {
                if (i % 2 == 0) {
                    qrCode[6][i] = PIXEL_ONE;
                } else {
                    qrCode[6][i] = PIXEL_ZERO;
                }
            }
        }
    }

    private static void fillAlignPattern(char[][] qrCode) {
        if (qrCode.length == 25) {
            for (int i = 0; i < 5; i++) {
                System.arraycopy(QR_ALIGN_PATTERN[i], 0, qrCode[16 + i], 16, 5);
            }
        }
    }
}
