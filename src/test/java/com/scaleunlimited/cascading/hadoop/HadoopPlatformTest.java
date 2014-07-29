package com.scaleunlimited.cascading.hadoop;

import java.io.File;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.AbstractPlatformTest;
import com.scaleunlimited.cascading.hadoop.test.MiniClusterPlatform;

public class HadoopPlatformTest extends AbstractPlatformTest {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testTempPath() throws Exception {
        BasePlatform platform = new HadoopPlatform(HadoopPlatformTest.class);
        
        BasePath tempDir = platform.getTempDir();
        
        // Verify we can write and then read
        BasePath testDir = platform.makePath(tempDir, UUID.randomUUID().toString());

        Scheme scheme = platform.makeBinaryScheme(new Fields("name", "age"));
        Tap tap = platform.makeTap(scheme, testDir);
        TupleEntryCollector writer = tap.openForWrite(platform.makeFlowProcess());
        writer.add(new Tuple("ken", 37));
        writer.close();

        TupleEntryIterator iter = tap.openForRead(platform.makeFlowProcess());
        assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        assertEquals("ken", te.getString("name"));
        assertFalse(iter.hasNext());
        iter.close();
    }
    
    @Test
    public void testPathCreation() throws Exception {
        // Clear it out first.
        final String targetDirname = "build/test/HadoopPlatformTest/testPathCreation";
        File targetDirFile = new File(targetDirname);
        FileUtils.deleteDirectory(targetDirFile);
        assertFalse(targetDirFile.exists());
        
        BasePlatform platform = new HadoopPlatform(HadoopPlatformTest.class);
        
        BasePath path = platform.makePath(targetDirname);
        assertEquals(targetDirname, path.getPath());
        assertEquals(targetDirFile.toURI().toString(), path.getAbsolutePath());
        assertEquals(targetDirFile.toURI().toString(), path.toString());
        
        assertFalse(path.exists());
        assertTrue(path.mkdirs());
        assertTrue(path.isDirectory());
        assertFalse(path.isFile());

        assertTrue(targetDirFile.exists());
        assertTrue(targetDirFile.isDirectory());

        // Check out sub-dir support.
        File subDirFile = new File(targetDirFile, "subdir");

        BasePath child = platform.makePath(path, "subdir");
        assertEquals(targetDirname + "/" + "subdir", child.getPath());
        assertEquals(subDirFile.toURI().toString(), child.getAbsolutePath());
        
        assertFalse(child.exists());
        assertTrue(child.mkdirs());
        assertTrue(child.isDirectory());
        assertFalse(child.isFile());
        
        assertTrue(subDirFile.exists());
        assertTrue(subDirFile.isDirectory());
    }

    @Test
    public void testRename() throws Exception {
        BasePlatform platform = new HadoopPlatform(HadoopPlatformTest.class);
        final String targetDirname = "build/test/HadoopPlatformTest/testRename";
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
        
        assertFalse(src.exists());
        assertTrue(dst.exists());
        assertEquals("dst", dst.getName());
        
        
        platform = new MiniClusterPlatform(HadoopPlatformTest.class);
        platform.rename(dst, src);
        assertFalse(dst.exists());
        assertTrue(src.exists());
        assertEquals("src", src.getName());
       
        BasePath subDir = platform.makePath(path, "dst/subDir");
        platform.rename(src, subDir);
        assertFalse(src.exists());
        assertTrue(dst.exists());
        assertTrue(subDir.exists());
        assertEquals("subDir", subDir.getName());
        
        BasePath aFile = platform.makePath(subDir, "aFile");
        assertFalse(aFile.exists());
        aFile.createNewFile();
        assertTrue(aFile.exists());
        
        src.mkdirs();
        BasePath  bFile = platform.makePath(src, "bFile");
        assertFalse(bFile.exists());
        platform.rename(aFile, bFile);
        assertFalse(aFile.exists());
        assertTrue(bFile.exists());
        

    }

    @Test
    public void testSerialization() throws Exception {
        HadoopPlatform platform = new HadoopPlatform(HadoopPlatformTest.class);
        platform.setMapSpeculativeExecution(true);
        platform.setProperty("my.bogus.property", 999);
        
        testSerialization(platform);
    }

    @Test
    public void testPlatformType() throws Exception {
        BasePlatform platform = new HadoopPlatform(HadoopPlatformTest.class);
        assertEquals(HadoopPlatform.PLATFORM_TYPE, platform.getPlatformType());
    }
}
