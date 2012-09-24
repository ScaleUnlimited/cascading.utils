/**
 * Copyright 2010 TransPac Software, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.scaleunlimited.cascading;

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
