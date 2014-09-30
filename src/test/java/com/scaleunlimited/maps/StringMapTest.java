package com.scaleunlimited.maps;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.hadoop.HadoopPlatform;
import com.scaleunlimited.cascading.local.LocalPlatform;

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
        
        final int numKeys = 500000;
        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            assertFalse(sm.containsKey(s));
            assertNull(sm.put(s, s));
            assertTrue(sm.containsKey(s));
            assertEquals(s, sm.put(s, s));
        }
        
        assertEquals(numKeys, sm.size());
        
        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            assertTrue("Contains key " + s, sm.containsKey(s));
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
        
        assertEquals(numKeys, sm.size());
        
        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            assertTrue(sm.containsKey(s));
        }
    }
    
    @Test
    public void testSmallSerialization() throws Exception {
        StringMap sm = new StringMap(true);
        assertNull(sm.put("key", "value"));
        assertEquals("value", sm.put("key", "value-1000"));
        
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
        assertEquals("value-1000", sm2.get("key"));
    }
    
    @Test
    public void testBigSerialization() throws Exception {
        StringMap sm = new StringMap();
        
        final int numKeys = 100000;
        for (int i = 0; i < numKeys; i++) {
            String s = "test-" + i;
            String v = "value-" + i;
            assertFalse(sm.containsKey(s));
            assertNull(sm.put(s, v));
            assertTrue(sm.containsKey(s));
            
            // Every so often we want to put a different value.
            if ((i % 1000) == 0) {
                String v2 = "value-" + (numKeys + i);
                assertEquals(v, sm.put(s,  v2));
                assertEquals(v2, sm.get(s));
            }
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
            String v = "value-" + (((i % 1000) == 0) ? (numKeys + i) : i);
            assertTrue("Must contain " + s, sm2.containsKey(s));
            assertEquals("Must have correct value for key " + i, v, sm2.get(s));
        }
    }
    

    /**
     * Test case to verify fix for issue seen using StringMap in workflow.
     * 
     * @throws Throwable
     */
    @Test
    public void testCream() throws Throwable {
        StringMap sm = new StringMap(true);
        int numTestKeys = 1000;
        int numCreamPuts = 8;
        for (int i = 0; i < numTestKeys; i++) {
            String s = "test-" + i;
            assertFalse(sm.containsKey(s));
            assertNull(sm.put(s, s));
            assertTrue(sm.containsKey(s));
            assertEquals(s, sm.put(s, s));
            if ((i % (numTestKeys / numCreamPuts)) == 0) {
                sm.put("cream", "0CREAM");
            }
        }
        assertEquals("0CREAM", sm.get("cream"));
        
        File dest = new File("build/test/StringMapTest/testCream/string.map");
        LocalPlatform localPlatform = new LocalPlatform(StringMapTest.class);
        BasePath smDir = localPlatform.makePath(dest.getParentFile().getAbsolutePath());
        smDir.mkdirs();
        BasePath smPath = localPlatform.makePath(smDir, dest.getName());
        
        serialize(sm, smPath);
        StringMap sm2 = deserialize(smPath);
        
        assertEquals("test-0", "test-0");
        assertEquals("0CREAM", sm2.get("cream"));
        
        HadoopPlatform hadoopPlatform = new HadoopPlatform(StringMapTest.class);
        smPath = hadoopPlatform.makePath(dest.getAbsolutePath());
        
        serialize(sm, smPath);
        sm2 = deserialize(smPath);
        
        assertEquals("test-0", "test-0");
        assertEquals("0CREAM", sm2.get("cream"));
    }

    protected void serialize(StringMap sm, BasePath smPath) throws IOException {
        OutputStream os = null;
        DataOutputStream dos = null;
        try {
            os = smPath.openOutputStream();
            dos = new DataOutputStream(os);
            sm.write(dos);
        } finally {
            IOUtils.closeQuietly(os);
            IOUtils.closeQuietly(dos);
        }
    }

    protected StringMap deserialize(BasePath smPath) throws IOException {
        InputStream is = null;
        DataInputStream dis = null;
        StringMap sm2 = new StringMap();
        try {
            is = smPath.openInputStream();
            dis = new DataInputStream(is);
            sm2.readFields(dis);
        } finally {
            IOUtils.closeQuietly(dis);
            IOUtils.closeQuietly(is);
        }
        return sm2;
    }

    @Test
    public void testUpdateSerialization() throws Exception {
        StringMap sm = new StringMap(true);

        String key = "key";
        assertFalse(sm.containsKey(key));
        String curValue = null;
        for (int i = 0; i < 10000; i++) {
            String newValue = "value-" + i;
            assertEquals(curValue, sm.put(key, newValue));
            curValue = newValue;
        }
        
        File dir = new File("build/test/StringMapTest/testUpdateSerialization/");
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
        assertEquals(curValue, sm2.get(key));
        
        // Now let's make sure we get back null for every non-key.
        for (int i = 0; i < 100000; i++) {
            assertNull(sm2.get("key-"+i));
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
            String v = "value-" + i;
            assertTrue(sm2.containsKey(s));
            assertEquals(v, sm2.get(s));
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
