package com.scaleunlimited.cascading.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.scaleunlimited.cascading.BasePath;

public class HadoopPath extends BasePath {
    private static final Logger LOGGER = Logger.getLogger(HadoopPath.class);
    
    private Configuration _conf;
    private Path _hadoopPath;
    private FileSystem _hadoopFS;
    
    public HadoopPath(String path) throws IOException {
        this(path, new Configuration());
    }
    
    public HadoopPath(String path, Configuration conf) throws IOException {
        super(path);
        
        _conf = conf;
        
        Path relativePath = new Path(path);
        _hadoopFS = relativePath.getFileSystem(_conf);
        if (!relativePath.isAbsolute()) {
            Path parent = _hadoopFS.getFileStatus(new Path(".")).getPath(); 
            _hadoopPath = new Path(parent, relativePath);
        } else {
            _hadoopPath = relativePath;
        }
    }
    
    public HadoopPath(BasePath parent, String subdir) throws IOException {
        this(parent, subdir, new Configuration());
    }
    
    public HadoopPath(BasePath parent, String subdir, Configuration conf) throws IOException {
        this(subdir);
        
        Path parentPath = new Path(parent.getAbsolutePath());
        _hadoopFS = parentPath.getFileSystem(conf);
        _hadoopPath = new Path(parentPath, subdir);
    }
    
    public Path getHadoopPath() {
        return _hadoopPath;
    }
    
    @Override
    public String getAbsolutePath() {
        return _hadoopPath.toString();
    }

    @Override
    public boolean isFile() {
        try {
            return _hadoopFS.isFile(_hadoopPath);
        } catch (IOException e) {
            LOGGER.error("Exception getting information about Hadoop path: " + e.getMessage(), e);
            return false;
        }
    }

    @Override
    public boolean isDirectory() {
        try {
            return _hadoopFS.getFileStatus(_hadoopPath).isDir();
        } catch (IOException e) {
            LOGGER.error("Exception getting information about Hadoop path: " + e.getMessage(), e);
            return false;
        }
    }

    @Override
    public boolean exists() {
        try {
            return _hadoopFS.exists(_hadoopPath);
        } catch (IOException e) {
            LOGGER.error("Exception getting information about Hadoop path: " + e.getMessage(), e);
            return false;
        }
    }

    @Override
    public boolean mkdirs() {
        try {
            return _hadoopFS.mkdirs(_hadoopPath);
        } catch (IOException e) {
            LOGGER.error("Exception creating directory for Hadoop path: " + e.getMessage(), e);
            return false;
        }
    }
    
    @Override
    public boolean delete(boolean isRecursive) {
        try {
            return _hadoopFS.delete(_hadoopPath, isRecursive);
        } catch (IOException e) {
            LOGGER.error("Exception deleting Hadoop path: " + e.getMessage(), e);
            return false;
        }
    }
    
    @Override
    public String getName() {
        return _hadoopPath.getName();
    }
    
    @Override
    public BasePath[] list() throws Exception {
        FileStatus[] files = _hadoopFS.listStatus(_hadoopPath);
        HadoopPath[] result = new HadoopPath[files.length];
        for (int i = 0; i < files.length; i++) {
            result[i] = new HadoopPath(files[i].getPath().toString(), _conf);
        }
        
        return result;
    }
    
    @Override
    public String toString() {
        return getAbsolutePath();
    }

}
