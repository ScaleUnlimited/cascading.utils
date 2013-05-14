package com.scaleunlimited.cascading.hadoop;

import static org.junit.Assert.*;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;

public class HadoopPlatformTest {

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
