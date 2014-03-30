package com.scaleunlimited.maps;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.junit.Test;

public class StringSetTest {

    @Test
    public void test() {
        StringSet ss = new StringSet();
        
        assertFalse(ss.contains("test"));
        assertTrue(ss.add("test"));
        assertTrue(ss.contains("test"));
        assertFalse(ss.add("test"));
        
        ss.clear();
        assertFalse(ss.contains("test"));
    }
    
    @Test
    public void testBigData() {
        StringSet ss = new StringSet();
        
        for (int i = 0; i < 1000000; i++) {
            String s = "test-" + i;
            assertFalse(ss.contains(s));
            assertTrue(ss.add(s));
            assertTrue(ss.contains(s));
            assertFalse(ss.add(s));
        }
        
        System.out.println("Set size is: " + ss.size());
        
        for (int i = 0; i < 1000000; i++) {
            String s = "test-" + i;
            assertTrue(ss.contains(s));
        }
    }
    
    @Test
    public void testCollisionSet() {
        StringSet ss = new StringSet(true);
        
        final int numKeys = 1000;
        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            assertFalse(ss.contains(s));
            assertTrue(ss.add(s));
            assertTrue(ss.contains(s));
            assertFalse(ss.add(s));
        }
        
        System.out.println("Set size is: " + ss.size());
        
        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            assertTrue(ss.contains(s));
        }
    }
    
    @Test
    public void testSmallSerialization() throws Exception {
        StringSet ss = new StringSet();
        ss.add("key");
        
        File dir = new File("build/test/StringSetTest/testSmallSerialization/");
        dir.mkdirs();
        File file = new File(dir, "string.set");
        file.delete();
        
        OutputStream os = new FileOutputStream(file);
        DataOutputStream out = new DataOutputStream(os);
        ss.write(out);
        out.close();
        
        StringSet ss2 = new StringSet();
        InputStream is = new FileInputStream(file);
        DataInputStream in = new DataInputStream(is);
        ss2.readFields(in);
        
        assertEquals(1, ss2.size());
        assertTrue(ss2.contains("key"));
    }
    
    @Test
    public void testBigSerialization() throws Exception {
        StringSet ss = new StringSet();
        
        final int numKeys = 1000000;
        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            assertFalse(ss.contains(s));
            assertTrue(ss.add(s));
            assertTrue(ss.contains(s));
            assertFalse(ss.add(s));
        }

        File dir = new File("build/test/StringSetTest/testBigSerialization/");
        dir.mkdirs();
        File file = new File(dir, "string.set");
        file.delete();
        
        OutputStream os = new FileOutputStream(file);
        DataOutputStream out = new DataOutputStream(os);
        ss.write(out);
        out.close();
        
        StringSet ss2 = new StringSet();
        InputStream is = new FileInputStream(file);
        DataInputStream in = new DataInputStream(is);
        ss2.readFields(in);
        
        assertEquals(numKeys, ss2.size());
        
        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            assertTrue(ss2.contains(s));
        }
    }
    
    @Test
    public void testCollisionSerialization() throws Exception {
        StringSet ss = new StringSet(true);
        
        final int numKeys = 1000;
        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            assertFalse(ss.contains(s));
            assertTrue(ss.add(s));
            assertTrue(ss.contains(s));
            assertFalse(ss.add(s));
        }
        
        File dir = new File("build/test/StringSetTest/testCollisionSerialization/");
        dir.mkdirs();
        File file = new File(dir, "string.set");
        file.delete();
        
        OutputStream os = new FileOutputStream(file);
        DataOutputStream out = new DataOutputStream(os);
        ss.write(out);
        out.close();
        
        StringSet ss2 = new StringSet();
        InputStream is = new FileInputStream(file);
        DataInputStream in = new DataInputStream(is);
        ss2.readFields(in);
        
        assertEquals(numKeys, ss2.size());

        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            assertTrue(ss2.contains(s));
        }
    }
    
    @Test
    public void testRemovingEntry() throws Exception {
        StringSet ss = new StringSet();
        assertTrue(ss.add("key"));
        assertTrue(ss.remove("key"));
        assertFalse(ss.remove("key"));
        assertFalse(ss.contains("key"));
        
        File dir = new File("build/test/StringSetTest/testRemovingEntry/");
        dir.mkdirs();
        File file = new File(dir, "string.set");
        file.delete();
        
        OutputStream os = new FileOutputStream(file);
        DataOutputStream out = new DataOutputStream(os);
        ss.write(out);
        out.close();
        
        StringSet ss2 = new StringSet();
        InputStream is = new FileInputStream(file);
        DataInputStream in = new DataInputStream(is);
        ss2.readFields(in);
        
        assertEquals(0, ss2.size());
        assertFalse(ss2.contains("key"));
    }
    

}
