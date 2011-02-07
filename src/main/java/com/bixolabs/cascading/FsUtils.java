/**
 * Copyright (c) 2009-2010 TransPac Software, Inc.
 * All rights reserved.
 *
 */
package com.bixolabs.cascading;

import java.io.File;
import java.io.IOException;
import java.security.InvalidParameterException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FsUtils {

    /**
     * Protect against earlier versions of Hadoop returning null if there
     * are no sub-directories in <path>
     * 
     * @param fs
     * @param path
     * @return
     * @throws IOException
     */
    public static FileStatus[] listStatus(FileSystem fs, Path path) throws IOException {
        FileStatus[] result = fs.listStatus(path);
        if (result == null) {
            result = new FileStatus[0];
        }
        
        return result;
    }
    

    public static File makeTempDir() throws IOException {
        File tmpFile = File.createTempFile("temp-dir", "tmp");
        tmpFile.deleteOnExit();

        String filename = tmpFile.getCanonicalPath();
        String dirname = filename.substring(0, filename.length() - 4);
        File tmpDir = new File(dirname);
        if (!tmpDir.mkdir()) {
            throw new IOException("Can't create temp directory: " + dirname);
        }
        
        return tmpDir;
    }

    public static void assertPathExists(FileSystem fs,
                                        Path path,
                                        String description)
        throws IOException {
            
        if (!fs.exists(path)) {
            String message = String.format( "%s doesn't exist: %s",
                                            description,
                                            path);
            throw new InvalidParameterException(message);
        }
    }

}
