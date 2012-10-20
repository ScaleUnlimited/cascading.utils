package com.scaleunlimited.cascading.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

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
        
        BasePlatform platform = new LocalPlatform();
        
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

}
