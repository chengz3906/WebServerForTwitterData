package cmu.cc.team.spongebob.query1.qrcode;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;


public class QRCode {
    /**
     * Pixels in QR code template.
     */
    private static final char PIXEL_ZERO = '0';
    private static final char PIXEL_ONE = '1';
    private static final char PIXEL_UNFILLED = 'x';
    private static final char PIXEL_BOUNDARY = 'b';

    /**
     * QR code templates.
     */
    private final char[][] QR_V1_TEMPLATE;
    private final char[][] QR_V2_TEMPLATE;

    public QRCode() {
        QR_V1_TEMPLATE = buildQRCodeTemplate(21);
        QR_V2_TEMPLATE = buildQRCodeTemplate(25);
    }

    public String encode(String text) {
        return "";
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

    char[][] buildQRCodeTemplate(int size) {
        char[][] template = new char[size][size];

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
                    template[row][i] = line.charAt(i);
                }
                row++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        return template;
    }

    private byte errorCode(byte b) {
        return (byte) (Integer.bitCount((int) b) % 2);
    }
}
