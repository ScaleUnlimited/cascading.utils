package com.scaleunlimited.maps;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

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
        
        System.out.println("Map size is: " + sm.size());
        
        for (int i = 0; i < 1000000; i++) {
            String s = "test-" + i;
            assertTrue(sm.containsKey(s));
        }
    }
    
    @Test
    public void testCollisionMap() {
        StringMap sm = new StringMap(true);
        
        final int numKeys = 1000;
        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            assertFalse(sm.containsKey(s));
            assertNull(sm.put(s, s));
            assertTrue(sm.containsKey(s));
            assertEquals(s, sm.put(s, s));
        }
        
        System.out.println("Map size is: " + sm.size());
        
        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            assertTrue(sm.containsKey(s));
        }
    }
    
    @Test
    public void testSmallSerialization() throws Exception {
        StringMap sm = new StringMap(true);
        sm.put("key", "value");
        
        File dir = new File("build/test/StringMapTest/testSmallSerialization/");
        dir.mkdirs();
        File file = new File(dir, "string.map");
        file.delete();
        
        OutputStream os = new FileOutputStream(file);
        DataOutputStream out = new DataOutputStream(os);
        sm.write(out);
        out.close();
        
        StringMap sm2 = new StringMap();
        InputStream is = new FileInputStream(file);
        DataInputStream in = new DataInputStream(is);
        sm2.readFields(in);
        
        assertEquals(1, sm2.size());
        assertTrue(sm2.containsKey("key"));
        assertTrue(sm2.get("key").equals("value"));
    }
    
 // TODO KKr - enable once verified   @Test
    public void testBigSerialization() throws Exception {
        StringMap sm = new StringMap(true);
        
        final int numKeys = 100000;
        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            String v = "value-" + i;
            assertFalse(sm.containsKey(s));
            assertNull(sm.put(s, v));
            assertTrue(sm.containsKey(s));
            assertEquals(v, sm.put(s, v));

        }

        File dir = new File("build/test/StringMapTest/testBigSerialization/");
        dir.mkdirs();
        File file = new File(dir, "string.map");
        file.delete();
        
        OutputStream os = new FileOutputStream(file);
        DataOutputStream out = new DataOutputStream(os);
        sm.write(out);
        out.close();
        
        StringMap sm2 = new StringMap();
        InputStream is = new FileInputStream(file);
        DataInputStream in = new DataInputStream(is);
        sm2.readFields(in);
        
        assertEquals(numKeys, sm2.size());
        
        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            String v = "value-" + i;
            assertTrue(sm2.containsKey(s));
//  TODO KKr - verify that this works          assertTrue(sm2.get(s).equals(v));
        }
    }
    
    @Test
    public void testCollisionSerialization() throws Exception {
        StringMap sm = new StringMap(true);
        
        final int numKeys = 1000;
        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            String v = "value-" + i;
            assertFalse(sm.containsKey(s));
            assertNull(sm.put(s, v));
            assertTrue(sm.containsKey(s));
            assertNotNull(sm.put(s, v));
        }
        
        File dir = new File("build/test/StringMapTest/testCollisionSerialization/");
        dir.mkdirs();
        File file = new File(dir, "string.map");
        file.delete();
        
        OutputStream os = new FileOutputStream(file);
        DataOutputStream out = new DataOutputStream(os);
        sm.write(out);
        out.close();
        
        StringMap sm2 = new StringMap();
        InputStream is = new FileInputStream(file);
        DataInputStream in = new DataInputStream(is);
        sm2.readFields(in);
        
        assertEquals(numKeys, sm2.size());

        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            assertTrue(sm2.containsKey(s));
        }
    }
    
    @Test
    public void testRemovingEntry() throws Exception {
        StringMap sm = new StringMap();
        assertNull(sm.put("key", "value"));
        assertEquals("value", sm.remove("key"));
        assertNull(sm.remove("key"));
        assertFalse(sm.containsKey("key"));
        
        File dir = new File("build/test/StringMapTest/testRemovingEntry/");
        dir.mkdirs();
        File file = new File(dir, "string.map");
        file.delete();
        
        OutputStream os = new FileOutputStream(file);
        DataOutputStream out = new DataOutputStream(os);
        sm.write(out);
        out.close();
        
        StringMap sm2 = new StringMap();
        InputStream is = new FileInputStream(file);
        DataInputStream in = new DataInputStream(is);
        sm2.readFields(in);
        
        assertEquals(0, sm2.size());
        assertFalse(sm2.containsKey("key"));
    }
   

}
