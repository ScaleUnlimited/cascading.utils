package com.scaleunlimited.cascading.local;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.CompositeTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tap.local.io.TapFileOutputStream;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryChainIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.util.Util;

/**
 * A DirectoryTap is a Cascading local tap that represents a directory of files, similar to Lfs in the
 * Hadoop platform.
 * 
 */
@SuppressWarnings("serial")
public class DirectoryTap extends Tap<Properties, InputStream, OutputStream> implements CompositeTap<FileTap> {

    private class TupleIterator implements Iterator<Tuple> {
        final TupleEntryIterator iterator;

        private TupleIterator(TupleEntryIterator iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Tuple next() {
            return iterator.next().getTuple();
        }

        @Override
        public void remove() {
            iterator.remove();
        }
    }

    private String path;
    private List<FileTap> _taps;
    
    /**
     * Constructor FileTap creates a new FileTap instance using the given
     * {@link cascading.scheme.Scheme} and file {@code path}.
     * 
     * @param scheme
     *            of type LocalScheme
     * @param path
     *            of type String
     */
    public DirectoryTap(Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String path) {
        this(scheme, path, SinkMode.KEEP);
    }

    /**
     * Constructor FileTap creates a new FileTap instance using the given
     * {@link cascading.scheme.Scheme}, file {@code path}, and {@code SinkMode}.
     * 
     * @param scheme
     *            of type LocalScheme
     * @param path
     *            of type String
     * @param sinkMode
     *            of type SinkMode
     */
    public DirectoryTap(Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String path, SinkMode sinkMode) {
        super(scheme, sinkMode);
        this.path = path;
    }

    @Override
    public String getIdentifier() {
        return path;
    }

    public String getPath() {
        return path;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public TupleEntryIterator openForRead(FlowProcess<Properties> flowProcess, InputStream input) throws IOException {
        File dir = new File(path);
        
        if (!dir.exists()) {
            throw new IllegalArgumentException("Path provided doesn't exist: " + path);
        } else if (!dir.isDirectory()) {
            throw new IllegalArgumentException("Path provided isn't a directory: " + path);
        }
        
        File[] files = dir.listFiles();
        _taps = new ArrayList<FileTap>(files.length);
        
        for (File file : files) {
            if (file.isFile()) {
                _taps.add(new FileTap(getScheme(), file.getAbsolutePath()));
            }
        }

        // TODO what to do about input? Why does MultiSourceTap check for input != null, and return first tap's TEI?
        
        List<Iterator<Tuple>> iterators = new ArrayList<Iterator<Tuple>>();
        for (FileTap tap : _taps) {
            iterators.add(new TupleIterator(tap.openForRead(flowProcess)));
        }
        
        return new TupleEntryChainIterator(getSourceFields(), iterators.toArray(new Iterator[_taps.size()]));
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Properties> flowProcess, OutputStream output) throws IOException {
        if (output == null) {
            output = new DirectoryFileOutputStream(this, "part-00000", isUpdate()); // append
        }

        return new TupleEntrySchemeCollector<Properties, OutputStream>(flowProcess, getScheme(), output, path);
    }

    /**
     * Method getSize returns the size of the file referenced by this tap.
     * 
     * @param conf
     *            of type Properties
     * @return The size of the file reference by this tap.
     * @throws IOException
     */
    public long getSize(Properties conf) throws IOException {
        long totalSize = 0;
        for (FileTap tap : _taps) {
            totalSize += tap.getSize(conf);
        }

        return totalSize;
    }

    @Override
    public boolean createResource(Properties conf) throws IOException {
        File dir = new File(path);

        return dir.exists() || dir.mkdirs();
    }

    @Override
    public boolean deleteResource(Properties conf) throws IOException {
        FileUtils.deleteDirectory(new File(path));
        return true;
    }

    @Override
    public boolean commitResource(Properties conf) throws IOException {
        return true;
    }

    @Override
    public boolean resourceExists(Properties conf) throws IOException {
        for (FileTap tap : _taps) {
            if (!tap.resourceExists(conf)) {
                return false;
            }
        }
        
        return true;
    }

    @Override
    public long getModifiedTime(Properties conf) throws IOException {
        long modified = 0;
        
        for (FileTap tap : _taps) {
            modified = Math.max(modified, tap.getModifiedTime(conf));
        }

        return modified;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        
        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        return Arrays.equals(_taps.toArray(), ((DirectoryTap) object)._taps.toArray());
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (_taps != null ? Arrays.hashCode(_taps.toArray()) : 0);
        return result;
    }

    @Override
    public String toString() {
        if (path != null)
            return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[\"" + Util.sanitizeUrl(path) + "\"]"; // sanitize
        else
            return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[not initialized]";
    }

    @Override
    public Iterator<FileTap> getChildTaps() {
        return _taps.iterator();
    }

    @Override
    public long getNumChildTaps() {
        return _taps.size();
    }
}
