package com.scaleunlimited.cascading.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;

public class LocalPlatformTest {

    @Test
    public void testPathCreation() throws Exception {
        // Clear it out first.
        final String targetDirname = "build/test/LocalPlatformTest/testPathCreation";
        File targetDirFile = new File(targetDirname);
        FileUtils.deleteDirectory(targetDirFile);
        assertFalse(targetDirFile.exists());
        
        BasePlatform platform = new LocalPlatform(LocalPlatformTest.class);
        
        BasePath path = platform.makePath(targetDirname);
        assertEquals(targetDirname, path.getPath());
        assertEquals(targetDirFile.getAbsolutePath(), path.getAbsolutePath());
        assertEquals(targetDirFile.getAbsolutePath(), path.toString());
        
        assertFalse(path.exists());
        assertTrue(path.mkdirs());
        assertTrue(path.isDirectory());
        assertFalse(path.isFile());

        assertTrue(targetDirFile.exists());
        assertTrue(targetDirFile.isDirectory());

        // Check out sub-dir support.
        File subDirFile = new File(targetDirFile, "subdir");

        BasePath child = platform.makePath(path, "subdir");
        assertEquals("subdir", child.getPath());
        assertEquals(subDirFile.getAbsolutePath(), child.getAbsolutePath());
        
        assertFalse(child.exists());
        assertTrue(child.mkdirs());
        assertTrue(child.isDirectory());
        assertFalse(child.isFile());
        
        assertTrue(subDirFile.exists());
        assertTrue(subDirFile.isDirectory());
    }
    
    private static BasePlatform makePlatform(boolean testing) {
        return new LocalPlatform(LocalPlatformTest.class);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testBinaryScheme() throws Exception {
        boolean testing = true;
        BasePlatform platform = makePlatform(testing);
        final String targetDirname = "build/test/LocalPlatformTest/testPathCreation";
        BasePath path = platform.makePath(targetDirname);

        Scheme scheme = platform.makeBinaryScheme(new Fields("name", "age"));
        Tap tap = platform.makeTap(scheme, path);
        TupleEntryCollector writer = tap.openForWrite(platform.makeFlowProcess());
        writer.add(new Tuple("ken", 37));
        writer.close();
    }

    @Test
    public void testRename() throws Exception {
        boolean testing = true;
        BasePlatform platform = makePlatform(testing);
        final String targetDirname = "build/test/LocalPlatformTest/testRename";
        BasePath path = platform.makePath(targetDirname);
        if (path.exists()) {
            path.delete(true);
        }
        path.mkdirs();
        
        BasePath src = platform.makePath(path, "src");
        src.mkdirs();
        
        assertTrue(src.exists());
        
        BasePath dst = platform.makePath(path, "dst");
        assertFalse(dst.exists());
        platform.rename(src, dst);
        
        assertTrue(dst.exists());
        assertFalse(src.exists());
        
    }
}
