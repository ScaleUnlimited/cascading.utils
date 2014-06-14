package com.scaleunlimited.cascading.local;

import static org.junit.Assert.*;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;

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
        assertTrue(path.isLocal());
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
    
    @Test
    public void testNoSuchDirectory() throws Exception {
        BasePath path = new LocalPath("build/test/LocalPathTest/boguspathtononexistentdirectory");
        assertEquals(0, path.list().length);
    }
    
    @Test
    public void testInputOutput() throws Exception {
        // Clear it out first.
        final String targetDirname = "build/test/LocalPathTest/testInputOutput";
        File targetDirFile = new File(targetDirname);
        FileUtils.deleteDirectory(targetDirFile);
        assertFalse(targetDirFile.exists());
        
        BasePath path = new LocalPath(targetDirname);
        assertTrue(path.mkdirs());
        
        BasePath file = new LocalPath(path, "test.txt");
        assertFalse(file.exists());
        assertTrue(file.createNewFile());
        assertTrue(file.exists());
        assertTrue(file.isFile());
        
        final String sourceStr = "Hello world";
        OutputStream os = file.openOutputStream();
        os.write(sourceStr.getBytes("UTF-8"));
        os.close();
        
        InputStream is = file.openInputStream();
        byte[] buffer = new byte[1000];
        int len = is.read(buffer);
        String result = new String(buffer, 0, len, "UTF-8");
        assertEquals(sourceStr, result);
        is.close();
    }

}
