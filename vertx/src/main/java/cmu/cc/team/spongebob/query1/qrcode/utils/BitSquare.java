package cmu.cc.team.spongebob.query1.qrcode.utils;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.util.BitSet;


public class BitSquare {
    private final @Getter int size;
    private BitSet bitSet;

    public BitSquare(int size) {
        this.size = size;
        int length = size * size;
        bitSet = new BitSet(length);
    }

    public BitSquare(int size, BitSet bitset) {
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

    public BitSquare clone() {
        return new BitSquare(size, (BitSet) bitSet.clone());
    }

    public void add(BitSquare another) {
        bitSet.or(another.bitSet);
    }

    public void xor(BitSquare another) {
        bitSet.xor(another.bitSet);
    }

    public void and(BitSquare another) {
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

    // TODO optimize the search
    public BitSquare locateAndSlice(final BitSquare pattern) {
        BitSquare window = new BitSquare(pattern.size);

        for (int i = 0; i <= size - pattern.size; i++) {
            for (int j = 0; j <= size -  pattern.size; j++) {
                window.clear();
                cachedSlice(i, j, pattern.size, window);

                // check all rotations
                for (int k = 0; k <= 3; k++) {
                    // bit-wise and with the template
                    BitSquare masked = window.clone();
                    masked.and(pattern);

                    if (masked.equals(pattern)) { // patterns aligned
                        return window;
                    }

                    window.rotate90();
                }
            }
        }

        return null;
    }

    public boolean equals(BitSquare another) {
        return this.size == another.size && this.bitSet.equals(another.bitSet);
    }

    public static BitSquare fromHexString(String string, int size) {
        String[] tokens = StringUtils.splitByWholeSeparator(string, "0x");
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 * tokens.length);

        for (int i = 0; i < tokens.length; i++) {
            int fourBytes = (int) Long.parseLong(tokens[i], 16);

            // the last (size * size) % 32 bits are treated as an integer
            if (i == (tokens.length - 1) && (size * size) % 32 != 0) {
                    int leftShift = 32 - ((size * size) % 32);
                    fourBytes = fourBytes << leftShift;
            }
            byteBuffer.putInt(i * 4, fourBytes);
        }

        BitSet bits = BigEndianBitSet.valueOf(byteBuffer.array());
        return new BitSquare(size, bits);
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

    private void cachedSlice(int rowStart, int colStart, int sliceSize, BitSquare cache) {
        cache.clear();

        int firstInd = rowStart * this.size + colStart;
        int lastInd = (rowStart + sliceSize - 1) * this.size + colStart + sliceSize;

        int setBitInd = bitSet.nextSetBit(firstInd);
        int rowInSlice;
        int colInSlice;
        while (setBitInd != -1 && setBitInd < lastInd) {
            rowInSlice = setBitInd / size - rowStart;
            colInSlice = setBitInd % size - colStart;

            if (rowInSlice >= 0 &&  colInSlice >= 0
                    && rowInSlice < sliceSize && colInSlice < sliceSize) {
                cache.bitSet.set(rowInSlice * sliceSize + colInSlice);
            }

            setBitInd = bitSet.nextSetBit(setBitInd + 1);
        }
    }

    public String toStringPretty() {
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

    public void print() {
        System.out.println(this.toStringPretty());
    }

    private void clear() {
        bitSet.clear();
    }
}
