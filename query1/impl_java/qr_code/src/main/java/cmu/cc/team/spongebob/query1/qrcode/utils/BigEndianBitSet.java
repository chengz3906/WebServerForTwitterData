package cmu.cc.team.spongebob.query1.qrcode.utils;

import java.util.ArrayList;
import java.util.BitSet;


public class BigEndianBitSet {
    public static byte[] toByteArray(BitSet bitset) {
        byte[] smallEndianByteArray = bitset.toByteArray();
        byte[] bigEndianByteArray = new byte[smallEndianByteArray.length];
        for (int i = 0; i < bigEndianByteArray.length; i++) {
            bigEndianByteArray[i] = reverseBits(smallEndianByteArray[i]);
        }

        return bigEndianByteArray;
    }

    public static byte toByte(BitSet bitSet) {
        byte[] bytes = toByteArray(bitSet);
        return bytes[0];
    }

    public static BitSet valueOf(ArrayList<Byte> bytes) {
        byte[] bigEndianByteArray = new byte[bytes.size()];
        for (int i = 0; i < bytes.size(); i++) {
            byte reversed = reverseBits(bytes.get(i));
            bigEndianByteArray[i] = reversed;
        }
        return  BitSet.valueOf(bigEndianByteArray);
    }

    public static BitSet valueOf(byte[] bytes) {
        byte[] bigEndianByteArray = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            byte reversed = reverseBits(bytes[i]);
            bigEndianByteArray[i] = reversed;
        }
        return  BitSet.valueOf(bigEndianByteArray);
    }

    private static byte reverseBits(byte b) {
        return (byte) Integer.reverseBytes(Integer.reverse((int) b));
    }
}
