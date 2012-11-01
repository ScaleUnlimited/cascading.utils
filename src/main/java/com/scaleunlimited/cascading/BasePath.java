package com.scaleunlimited.cascading;

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
    public abstract boolean exists();
    public abstract boolean mkdirs();
    public abstract boolean delete(boolean isRecursive);
    
    public abstract String getName();
    public abstract BasePath[] list() throws Exception;
}
