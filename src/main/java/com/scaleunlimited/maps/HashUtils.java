package com.scaleunlimited.maps;

import java.io.UnsupportedEncodingException;

public class HashUtils {

    public static byte[] getUTF8Bytes(String str) {
        try {
            return str.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Impossible missing charset exception", e);
        }
    }
    
    /**
     * Generate a 32-bit JOAAT hash from the bytes of <s>
     * 
     * @param s String to hash
     * @return 32-bit hash
     */
    public static int getIntHash(String s) {
        byte[] bytes = getUTF8Bytes(s);
        return getIntHash(bytes, 0, bytes.length);
    }

    /**
     * Generate a 64-bit JOAAT hash from the bytes of <s>
     * 
     * @param s String to hash
     * @return 64-bit hash
     */
    public static long getLongHash(String s) {
        byte[] bytes = getUTF8Bytes(s);
        return getLongHash(bytes, 0, bytes.length);
    }

    /**
     * Generate a 64-bit JOAAT hash from the given byte array
     * 
     * @param b Bytes to hash
     * @param offset starting offset
     * @param length number of bytes to hash
     * @return 64-bit hash
     */
    public static long getLongHash(byte[] b, int offset, int length) {
        long result = 0;

        for (int i = 0; i < length; i++) {
            byte curByte = b[offset + i];
            int h = (int)curByte;
            
            result += h & 0x0FFL;
            result += (result << 20);
            result ^= (result >> 12);
        }
        
        result += (result << 6);
        result ^= (result >> 22);
        result += (result << 30);

        return result;
    }

    /**
     * Generate a 32-bit JOAAT hash for the given byte array
     * 
     * @param b Bytes to hash
     * @param offset starting offset
     * @param length number of bytes to hash
     * @return 32-bit hash
     */
    public static int getIntHash(byte[] b, int offset, int length) {
        int result = 0;

        for (int i = 0; i < length; i++) {
            byte curByte = b[offset + i];
            int h = (int)curByte;
            
            result += h & 0x0FFL;
            result += (result << 10);
            result ^= (result >> 6);
        }
        
        result += (result << 3);
        result ^= (result >> 11);
        result += (result << 15);

        return result;
    }

}
