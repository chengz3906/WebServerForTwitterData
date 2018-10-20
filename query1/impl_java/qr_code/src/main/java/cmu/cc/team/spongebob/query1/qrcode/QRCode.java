package cmu.cc.team.spongebob.query1.qrcode;

import java.nio.ByteBuffer;
import java.util.BitSet;

import lombok.Getter;


class QRCode {
    private final @Getter
    BitSet bitSet;
    private final @Getter int size;

    QRCode(int size) {
        this.size = size;
        int length = size * size;
        bitSet = new BitSet(length);
    }

    void setBit(int r, int c) {
        int ind = r * size + c;
        bitSet.set(ind);
    }

    boolean getBit(int r, int c) {
        int ind = r * size + c;
        return bitSet.get(ind);
    }

    void add(QRCode another) {
        bitSet.or(another.bitSet);
    }

    public String toString() {
        byte[] bytes = new byte[this.getBitSet().toByteArray().length];
        byte[]
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = reverseBits(qrCode.getBitSet().toByteArray()[i]);
        }
        for (int i = 0; i <= (bytes.length / 4); i++) {
            byte[] slice = new byte[]{(byte) 0, (byte) 0, (byte) 0, (byte) 0};

            System.arraycopy(bytes, i * 4, slice, 0, Math.min(4, bytes.length - (4 * i)));

            encoding.append("0x");
            int x = ByteBuffer.wrap(slice).getInt();

            // last set of bits
            if (i == bytes.length / 4) {
                x = x >> (32 - (21 * 21 % 32));
            }

            encoding.append(String.format("%x", x).toLowerCase());
        }
    }

    void print() {
        System.out.println("start");
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                if (getBit(i, j)) {
                    System.out.print('1');
                } else {
                    System.out.print('0');
                }
                System.out.print(' ');
            }
            System.out.println();
        }
        System.out.println("end");
    }
}