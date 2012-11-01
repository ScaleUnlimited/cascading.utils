package com.scaleunlimited.cascading.local;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import cascading.tap.Tap;
import cascading.tap.TapException;

public class DirectoryFileOutputStream extends FileOutputStream {

    private File _file;

    public DirectoryFileOutputStream(String path, boolean append) throws FileNotFoundException {
        this(null, path, append);
    }

    public DirectoryFileOutputStream(Tap parent, String path, boolean append) throws FileNotFoundException {
        super(prepare(parent, path), append);

        _file = getFile(parent, path);
    }

    public String asDirectory() throws IOException {

        // We need to close down the current file, delete it, and return back the
        close();

        if (!_file.delete()) {
            throw new IOException("Can't delete output file: " + _file);
        }

        return _file.getAbsolutePath();
    }

    private static String prepare(Tap parent, String path) {
        
        // ignore the output. will catch the failure downstream if any.
        // not ignoring the output causes race conditions with other systems
        // writing to the same directory.
        File file = getFile(parent, path);
        File parentFile = file.getAbsoluteFile().getParentFile();

        if (parentFile != null && parentFile.exists() && parentFile.isFile()) {
            throw new TapException("cannot create parent directory, it already exists as a file: " + parentFile.getAbsolutePath());
        }
        
        // don't test for success, just fighting a race condition otherwise
        // will get caught downstream
        if (parentFile != null) {
            parentFile.mkdirs();
        }
        
        return file.getAbsolutePath();
    }
    
    private static File getFile(Tap parent, String path) {
        if (parent == null) {
            return new File(path);
        } else {
            return new File(parent.getIdentifier(), path);
        }
    }

}
