package com.scaleunlimited.cascading;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * An abstract "path" to a file or a directory, with concrete classes
 * for Hadoop (Path) and local (File).
 *
 */
public abstract class BasePath {
    
    private String _path;
    
    public BasePath(String path) {
        _path = path;
    }
    
    public String getPath() {
        return _path;
    }
    
    public abstract String getAbsolutePath();
    public abstract boolean isFile();
    public abstract boolean isDirectory();
    public abstract boolean isLocal();
    public abstract boolean exists();
    public abstract boolean mkdirs();
    public abstract boolean createNewFile() throws IOException;
    public abstract boolean delete(boolean isRecursive);
    
    public abstract String getName();
    public abstract BasePath[] list() throws Exception;
    
    public abstract InputStream openInputStream() throws IOException;
    public abstract OutputStream openOutputStream() throws IOException;
}
