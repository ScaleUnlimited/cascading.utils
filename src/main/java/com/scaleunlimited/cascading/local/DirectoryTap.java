package com.scaleunlimited.cascading.local;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.CompositeTap;
import cascading.tap.SinkMode;
import cascading.tap.local.FileTap;
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
public class DirectoryTap extends FileTap implements CompositeTap<FileTap> {

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

    private transient List<FileTap> _taps;
    
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
        super(scheme, path);
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
        super(scheme, path, sinkMode);
    }

    public String getPath() {
        return getIdentifier();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public TupleEntryIterator openForRead(FlowProcess<Properties> flowProcess, InputStream input) throws IOException {
        // TODO what to do about input? Why does MultiSourceTap check for input != null, and return first tap's TEI?
        
        List<FileTap> taps = getTaps();
        List<Iterator<Tuple>> iterators = new ArrayList<Iterator<Tuple>>();
        for (FileTap tap : taps) {
            iterators.add(new TupleIterator(tap.openForRead(flowProcess)));
        }
        
        return new TupleEntryChainIterator(getSourceFields(), iterators.toArray(new Iterator[taps.size()]));
    }

    private List<FileTap> getTaps() {
        if (_taps == null) {
            File dir = new File(getPath());
            List<FileTap> result = new ArrayList<FileTap>();

            if (!dir.exists()) {
                throw new IllegalArgumentException("Path provided doesn't exist: " + getPath());
            } else if (dir.isDirectory()) {
                File[] files = dir.listFiles();

                for (File file : files) {
                    // Ignore .xxx files, like .part-00000.crc
                    if (file.isFile() && (!file.getName().startsWith("."))) {
                        result.add(new FileTap(getScheme(), file.getAbsolutePath()));
                    }
                }
            } else if (dir.isFile()) {
                result.add(new FileTap(getScheme(), dir.getAbsolutePath()));
            } else {
                throw new IllegalArgumentException("Path provided isn't a directory or a file: " + getPath());
            }

            _taps = result;
        }
        
        return _taps;
    }
    
    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Properties> flowProcess, OutputStream output) throws IOException {
        if (output == null) {
            if (getSinkMode() == SinkMode.REPLACE) {
                File dirFile = new File(getPath());
                if (dirFile.exists()) {
                    if (dirFile.isDirectory()) {
                        FileUtils.deleteDirectory(dirFile);
                    } else {
                        dirFile.delete();
                    }
                }
            }
            
            output = new DirectoryFileOutputStream(this, "part-00000", isUpdate());
        }

        return new TupleEntrySchemeCollector<Properties, OutputStream>(flowProcess, getScheme(), output, getPath());
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
        for (FileTap tap : getTaps()) {
            totalSize += tap.getSize(conf);
        }

        return totalSize;
    }

    @Override
    public boolean createResource(Properties conf) throws IOException {
        File dir = new File(getPath());

        return dir.exists() || dir.mkdirs();
    }

    @Override
    public boolean deleteResource(Properties conf) throws IOException {
        FileUtils.deleteDirectory(new File(getPath()));
        return true;
    }

    @Override
    public boolean commitResource(Properties conf) throws IOException {
        return true;
    }

    @Override
    public boolean resourceExists(Properties conf) throws IOException {
        // First see if the actual path exists, and is a directory.
        File pathDir = new File(getPath());
        if (!pathDir.exists()) {
            return false;
        } else if (pathDir.isFile()) {
            return true;
        }
        
        // Now we have to check for what's inside of the pathDir.
        for (FileTap tap : getTaps()) {
            if (!tap.resourceExists(conf)) {
                return false;
            }
        }
        
        return true;
    }

    @Override
    public long getModifiedTime(Properties conf) throws IOException {
        long modified = 0;
        
        for (FileTap tap : getTaps()) {
            modified = Math.max(modified, tap.getModifiedTime(conf));
        }

        return modified;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        DirectoryTap other = (DirectoryTap) obj;
        if (getPath() == null) {
            if (other.getPath() != null)
                return false;
        } else if (!getPath().equals(other.getPath()))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((getPath() == null) ? 0 : getPath().hashCode());
        return result;
    }

    @Override
    public String toString() {
        if (getPath() != null)
            return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[\"" + Util.sanitizeUrl(getPath()) + "\"]"; // sanitize
        else
            return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[not initialized]";
    }

    @Override
    public Iterator<FileTap> getChildTaps() {
        return getTaps().iterator();
    }

    @Override
    public long getNumChildTaps() {
        return getTaps().size();
    }
}
