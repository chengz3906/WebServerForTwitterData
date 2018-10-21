package cmu.cc.team.spongebob.query1.qrcode.utils;

import java.nio.ByteBuffer;
import java.util.BitSet;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;


public class BinaryMap {
    private final @Getter int size;
    private BitSet bitSet;

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

    public static BinaryMap valueOf(String string, int size) {
        String[] tokens = StringUtils.splitByWholeSeparator(string, "0x");
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 * tokens.length);

        for (int i = 0; i < tokens.length; i++) {
            int fourByte = (int) Long.parseLong(tokens[i], 16);

            if (i == (tokens.length - 1) && (size * size) % 32 != 0) {
                    int leftShift = 32 - ((size * size) % 32);
                    fourByte = fourByte << leftShift;
            }

            byteBuffer.putInt(i * 4, fourByte);
        }

        BitSet bits = BigEndianBitSet.valueOf(byteBuffer.array());

        return new BinaryMap(size, bits);
    }

    public String toStringBinaryMap() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                if (getBit(i, j)) {
                    sb.append('1');
                } else {
                    sb.append('0');
                }
                sb.append(' ');
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    public void rotate(int degree) {
        for (int i = 0; i < degree / 90; i++) {
            rotate90();
        }
    }

    private void rotate90() {
        int i = bitSet.nextSetBit(0);
        BitSet rotated = new BitSet(size * size);

        while (i != -1) {
            int r = i / size;
            int c = i % size;

            int rRot = size - 1 - c;
            rotated.set(rRot * size + r);

            i = bitSet.nextSetBit(i + 1);
        }

        this.bitSet = rotated;
    }

    public void print() {
        System.out.println("start");
        System.out.println(this.toStringBinaryMap());
        System.out.println("end");
    }
}
