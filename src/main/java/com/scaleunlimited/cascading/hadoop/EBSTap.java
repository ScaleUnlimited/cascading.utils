package com.scaleunlimited.cascading.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeCollector;
import cascading.tap.type.FileType;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

@SuppressWarnings({ "rawtypes", "serial" })
public class EBSTap extends Tap<JobConf, RecordReader, OutputCollector> implements FileType<JobConf> {

    private String _path;
    
    private transient File _dir;
    
    public EBSTap(Scheme<JobConf, RecordReader, OutputCollector, ?, ?> scheme, String path, SinkMode sinkMode) {
        super(scheme, sinkMode);
        _path = path;
    }
    
    @Override
    public String[] getChildIdentifiers(JobConf conf) throws IOException {
        throw new UnsupportedOperationException("EBSTap can't be used for reading");
    }

    @Override
    public String[] getChildIdentifiers(JobConf conf, int depth, boolean fullyQualified) throws IOException {
        throw new UnsupportedOperationException("EBSTap can't be used for reading");
    }

    @Override
    public long getSize(JobConf conf) throws IOException {
        // Only for reading case
        return 0;
    }

    private File getDir() {
        if (_dir == null) {
            _dir = new File(_path).getAbsoluteFile();
        }
        
        return _dir;
    }
    
    @Override
    public boolean isDirectory(JobConf conf) throws IOException {
        return getDir().isDirectory();
    }

    @Override
    public boolean createResource(JobConf conf) throws IOException {
        return getDir().mkdirs();
    }

    @Override
    public boolean deleteResource(JobConf conf) throws IOException {
        return getDir().delete();
    }

    @Override
    public String getIdentifier() {
        return getDir().getAbsolutePath();
    }

    @Override
    public long getModifiedTime(JobConf conf) throws IOException {
        return getDir().lastModified();
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess, RecordReader reader) throws IOException {
        throw new UnsupportedOperationException("EBSTap can't be used for reading");
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess, OutputCollector output) throws IOException {
        return new HadoopTupleEntrySchemeCollector(flowProcess, this, output);
    }

    @Override
    public boolean resourceExists(JobConf conf) throws IOException {
        return getDir().exists();
    }

}
