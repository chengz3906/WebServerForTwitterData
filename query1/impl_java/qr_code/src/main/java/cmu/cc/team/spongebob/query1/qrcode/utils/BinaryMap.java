package cmu.cc.team.spongebob.query1.qrcode.utils;

import java.nio.ByteBuffer;
import java.util.BitSet;

import lombok.Getter;


public class BinaryMap {
    private final @Getter
    BitSet bitSet;
    private final @Getter int size;

    public BinaryMap(int size) {
        this.size = size;
        int length = size * size;
        bitSet = new BigEndianBitSet(length);
    }

    public BinaryMap(int size, BitSet bitset) {
        this.bitSet = bitset;
        int length = size * size;

        // clear out of bound bits
        if (bitset.size() > length) {
            bitset.clear(length, bitset.size());
        }
        this.size = size;
    }

    public void setBit(int r, int c) {
        int ind = r * size + c;
        bitSet.set(ind);
    }

    public boolean getBit(int r, int c) {
        int ind = r * size + c;
        return bitSet.get(ind);
    }

    public void add(BinaryMap another) {
        bitSet.or(another.bitSet);
    }

    public void xor(BinaryMap another) {
        bitSet.xor(another.bitSet);
    }

    public String toString() {
        StringBuilder str = new StringBuilder();

        byte[] bytes = bitSet.toByteArray();
        for (int i = 0; i <= (bytes.length / 4); i++) {
            str.append("0x");

            byte[] slice = new byte[]{(byte) 0, (byte) 0, (byte) 0, (byte) 0};
            System.arraycopy(bytes, i * 4, slice, 0, Math.min(4, bytes.length - (4 * i)));
            int integer = ByteBuffer.wrap(slice).getInt();

            // last integer
            if (i == bytes.length / 4) {
                integer = integer >>> (32 - (size * size % 32));  // logical shift
            }

            str.append(String.format("%x", integer).toLowerCase());
        }

        return str.toString();
    }

    public static BinaryMap valueOf(String string) {
        return new BinaryMap(0);
    }

    public void print() {
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
