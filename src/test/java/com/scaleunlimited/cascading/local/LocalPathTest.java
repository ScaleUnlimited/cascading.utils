package com.scaleunlimited.cascading.local;

import static org.junit.Assert.*;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.hadoop.HadoopPath;

public class LocalPathTest {

    @Test
    public void test() throws Exception {
        // Clear it out first.
        final String targetDirname = "build/test/LocalPathTest/test";
        File targetDirFile = new File(targetDirname);
        FileUtils.deleteDirectory(targetDirFile);
        assertFalse(targetDirFile.exists());
        
        BasePath path = new LocalPath(targetDirname);
        assertEquals(targetDirname, path.getPath());
        assertEquals(targetDirFile.getAbsolutePath(), path.getAbsolutePath());
        assertEquals(targetDirFile.getAbsolutePath(), path.toString());
        
        assertFalse(path.exists());
        assertTrue(path.mkdirs());
        assertTrue(path.isDirectory());
        assertFalse(path.isFile());
        assertTrue(targetDirFile.exists());
        assertTrue(targetDirFile.isDirectory());
    }

}
