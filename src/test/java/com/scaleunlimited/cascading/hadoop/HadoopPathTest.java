package com.scaleunlimited.cascading.hadoop;


import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import com.scaleunlimited.cascading.BasePath;

public class HadoopPathTest extends Assert {

    @Test
    public void test() throws Exception {
        // Clear it out first.
        final String targetDirname = "build/test/HadoopPathTest/test";
        File targetDirFile = new File(targetDirname);
        FileUtils.deleteDirectory(targetDirFile);
        assertFalse(targetDirFile.exists());
        
        BasePath path = new HadoopPath(targetDirname);
        assertEquals(targetDirname, path.getPath());
        assertEquals(targetDirFile.toURI().toString(), path.getAbsolutePath());
        assertEquals(targetDirFile.toURI().toString(), path.toString());
        
        assertFalse(path.exists());
        assertTrue(path.mkdirs());
        assertTrue(path.isDirectory());
        assertFalse(path.isFile());
        assertTrue(targetDirFile.exists());
        assertTrue(targetDirFile.isDirectory());
    }

}
