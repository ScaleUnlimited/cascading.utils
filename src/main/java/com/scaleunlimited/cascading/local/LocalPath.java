package com.scaleunlimited.cascading.local;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.cascading.BasePath;

public class LocalPath extends BasePath {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalPath.class);

    private File _pathFile;
    
    public LocalPath(String path) {
        super(path);
        
        _pathFile = new File(path);
    }
    
    public LocalPath(BasePath parent, String subdir) {
        super(subdir);
        
        File parentFile = new File(parent.getAbsolutePath());
        _pathFile = new File(parentFile, subdir);
    }
    
    public File getPathFile() {
        return _pathFile;
    }
    
    @Override
    public String getAbsolutePath() {
        return _pathFile.getAbsolutePath();
    }

    @Override
    public boolean isFile() {
        return _pathFile.isFile();
    }

    @Override
    public boolean isDirectory() {
        return _pathFile.isDirectory();
    }

    @Override
    public boolean exists() {
        return _pathFile.exists();
    }

    @Override
    public boolean mkdirs() {
        return _pathFile.mkdirs();
    }
    
    @Override
    public boolean delete(boolean isRecursive) {
        try {
            if  (   _pathFile.isDirectory()
                &&  isRecursive) {
                FileUtils.deleteDirectory(_pathFile);
            } else {
                return _pathFile.delete();
            }
        } catch (IOException e) {
            LOGGER.error("Exception deleting local path: " + e.getMessage(), e);
            return false;
        }
        return true;
    }
    
    @Override
    public String getName() {
        return _pathFile.getName();
    }
    
    @Override
    public BasePath[] list() throws Exception {
        File[] paths = _pathFile.listFiles();
        if (paths == null) {
            return new BasePath[0];
        }
        
        LocalPath[] result = new LocalPath[paths.length];
        
        for (int i = 0; i < paths.length; i++) {
            result[i] = new LocalPath(paths[i].getAbsolutePath());
        }
        
        return result;
    }

    @Override
    public String toString() {
        return getAbsolutePath();
    }

    @Override
    public boolean isLocal() {
        return true;
    }

    @Override
    public boolean createNewFile() throws IOException {
        return _pathFile.createNewFile();
    }
    
    @Override
    public InputStream openInputStream() throws IOException {
        return new FileInputStream(_pathFile);
    }

    @Override
    public OutputStream openOutputStream() throws IOException {
        return new FileOutputStream(_pathFile);
    }


}
