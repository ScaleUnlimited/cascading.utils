package com.scaleunlimited.cascading;

import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.junit.Test;

public class UUIDWritableTest {

    /**
     * Ensure that {@link UUIDWritable#compareTo(UUIDWritable)} behaves the same
     * way as {@link UUID#compareTo(UUID)}.
     */
    @Test
    public void testCompareTo() {
        checkCompareTo( 0, 0,
                        0, 1);
        checkCompareTo( 1, 0,
                        1, 1);
        checkCompareTo( 0, -1,
                        1, 0);
        checkCompareTo( -1, -1,
                        0, 0);
        checkCompareTo( -1, -1,
                        Integer.MAX_VALUE, Integer.MAX_VALUE);
        checkCompareTo( Integer.MIN_VALUE, Integer.MIN_VALUE,
                        Integer.MAX_VALUE, Integer.MAX_VALUE);
        checkCompareTo( 0, 0,
                        0, 0);
        checkCompareTo( 0, -1,
                        0, -1);
        checkCompareTo( -1, -1,
                        -1, -1);
        checkCompareTo( Integer.MIN_VALUE, Integer.MIN_VALUE,
                        Integer.MIN_VALUE, Integer.MIN_VALUE);
        checkCompareTo( Integer.MAX_VALUE, Integer.MAX_VALUE,
                        Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    protected void checkCompareTo(  long hiBits1,
                                    long loBits1, 
                                    long hiBits2, 
                                    long loBits2) {
        UUID uuid1 = new UUID(hiBits1, loBits1);
        UUID uuid2 = new UUID(hiBits2, loBits2);
        UUIDWritable uuidWritable1 = new UUIDWritable(uuid1);
        UUIDWritable uuidWritable2 = new UUIDWritable(uuid2);
        int uuidResult = uuid1.compareTo(uuid2);
        int uuidWritableResult = uuidWritable1.compareTo(uuidWritable2);
        assertEquals(   (uuidResult == 0) ? 0 : (uuidResult / Math.abs(uuidResult)), 
                        (uuidWritableResult == 0) ? 0 : (uuidWritableResult / Math.abs(uuidWritableResult)));
        uuidResult = uuid2.compareTo(uuid1);
        uuidWritableResult = uuidWritable2.compareTo(uuidWritable1);
        assertEquals(   (uuidResult == 0) ? 0 : (uuidResult / Math.abs(uuidResult)), 
                        (uuidWritableResult == 0) ? 0 : (uuidWritableResult / Math.abs(uuidWritableResult)));
    }

}
