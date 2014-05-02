package com.scaleunlimited.maps;

import static org.junit.Assert.*;

import org.junit.Test;

public class StringMapTest {

    @Test
    public void test() {
        StringMap sm = new StringMap();
        
        assertFalse(sm.containsKey("test"));
        assertNull(sm.put("test", "value"));
        assertTrue(sm.containsKey("test"));
        assertEquals("value", sm.get("test"));
        
        assertEquals("value", sm.put("test", "value2"));
        assertEquals("value2", sm.get("test"));

        sm.clear();
        assertFalse(sm.containsKey("test"));
    }
    
    @Test
    public void testBigData() {
        StringMap sm = new StringMap();
        
        for (int i = 0; i < 1000000; i++) {
            String s = "test-" + i;
            assertFalse(sm.containsKey(s));
            assertNull(sm.put(s, s));
            assertTrue(sm.containsKey(s));
            assertEquals(s, sm.put(s, s));
        }
        
        System.out.println("Set size is: " + sm.size());
        
        for (int i = 0; i < 1000000; i++) {
            String s = "test-" + i;
            assertTrue(sm.containsKey(s));
        }
    }
    
    @Test
    public void testCollisionSet() {
        StringMap sm = new StringMap(true);
        
        final int numKeys = 1000;
        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            assertFalse(sm.containsKey(s));
            assertNull(sm.put(s, s));
            assertTrue(sm.containsKey(s));
            assertEquals(s, sm.put(s, s));
        }
        
        System.out.println("Set size is: " + sm.size());
        
        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            assertTrue(sm.containsKey(s));
        }
    }
    

}
