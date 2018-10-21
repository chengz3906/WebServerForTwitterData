package cmu.cc.team.spongebob.query1.qrcode.utils;

import java.nio.ByteBuffer;
import java.util.BitSet;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;


public class BinarySquare {
    private final @Getter int size;
    private BitSet bitSet;

    public BinarySquare(int size) {
        this.size = size;
        int length = size * size;
        bitSet = new BitSet(length);
    }

    public BinarySquare(int size, BitSet bitset) {
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

    public BinarySquare clone() {
        return new BinarySquare(size, (BitSet) bitSet.clone());
    }

    public void add(BinarySquare another) {
        bitSet.or(another.bitSet);
    }

    public void xor(BinarySquare another) {
        bitSet.xor(another.bitSet);
    }

    public void and(BinarySquare another) {
        bitSet.and(another.bitSet);
    }

    public String toString() {
        StringBuilder str = new StringBuilder();

        byte[] bytes = BigEndianBitSet.toByteArray(bitSet);
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

    public boolean equals(BinarySquare another) {
        return this.size == another.size && this.bitSet.equals(another.bitSet);
    }

    public static BinarySquare valueOf(String string, int size) {
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

        return new BinarySquare(size, bits);
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

    public BinarySquare slice(int rowStart, int colStart, int sliceSize) {
        BitSet slice =  new BitSet(sliceSize * sliceSize);

        int firstInd = rowStart * this.size + colStart;
        int lastInd = (rowStart + sliceSize - 1) * this.size + colStart + sliceSize;

        int setBitInd = bitSet.nextSetBit(firstInd);
        while (setBitInd != -1 && setBitInd < lastInd) {
            int r = setBitInd / size - rowStart;
            int c = setBitInd % size - colStart;

            if (r >= 0 &&  c >= 0 && r < sliceSize && c < sliceSize) {
                slice.set(r * sliceSize + c);
            }

            setBitInd = bitSet.nextSetBit(setBitInd + 1);
        }

        return new BinarySquare(sliceSize, slice);
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
        System.out.println(this.toStringBinaryMap());
    }
}
