package com.scaleunlimited.cascading.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.S3FileSystem;
import org.apache.log4j.Logger;

import com.scaleunlimited.cascading.BasePath;

public class HadoopPath extends BasePath {
    private static final Logger LOGGER = Logger.getLogger(HadoopPath.class);

    private static final String DISTCP_S3_FOLDER_MARKER_FILENAME_SUFFIX = "_$folder$";

    private static final long S3_DELETION_LATENCY_MILLISECONDS = 10 * 1000L;
    
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
            
            // Try really hard to make things easy for S3 folder deletions
            // by ensuring that folder names always end with a slash.
            Path parentPath = _hadoopPath.getParent();
            String targetName = _hadoopPath.getName();
            Path targetPath = _hadoopPath;
            boolean isFolder = !(_hadoopFS.isFile(targetPath));
            if  (   isFolder
                &&  (!(targetName.endsWith(Path.SEPARATOR)))) {
                targetName = targetName + Path.SEPARATOR;
                targetPath = new Path(parentPath, targetName);
                
            }
            boolean result = _hadoopFS.delete(targetPath, isRecursive);
            
            // Apparently, a folder delete in S3 can return true, even though
            // the folder might still exist. :(
            if (result) {
                
                // Give S3 some time to finish deleting the object so we
                // won't think the deletion failed when it actually
                // succeeded.
                if (_hadoopFS instanceof S3FileSystem) {
                    Thread.sleep(S3_DELETION_LATENCY_MILLISECONDS);
                }
                
                // Ensure that the special folder file associated with
                // (i.e., defining) S3 "folders" is always deleted as well.
                if  (   isFolder
                    &&  (_hadoopFS instanceof S3FileSystem)) {
                    String folderMarkerFileName =
                        (   targetName.substring(0, targetName.length()-1)
                        +   DISTCP_S3_FOLDER_MARKER_FILENAME_SUFFIX);
                    Path folderMarkerPath = 
                        new Path(parentPath, folderMarkerFileName);
                    if (_hadoopFS.exists(folderMarkerPath)) {
                        _hadoopFS.delete(folderMarkerPath, false);
                    }
                }
                if (exists()) {
                    return false;
                }
            }
            
            // Pass the result of the target deletion on to the caller
            return result;
            
        } catch (Exception e) {
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
