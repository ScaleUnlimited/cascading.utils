package com.scaleunlimited.cascading;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;

@SuppressWarnings("serial")
class NullScheme<Config> extends Scheme<Config, InputStream, Object, Reader, Writer> {
    
    public NullScheme() {
        super();
    }

    public NullScheme(Fields fields) {
        super(new Fields(), fields);
    }

    @Override
    public boolean isSink() {
        return true;
    }
    
    @Override
    public boolean isSource() {
        return false;
    }
    
    @Override
    public void sink(FlowProcess<Config> flowProcess, SinkCall<Writer, Object> sinkCall) throws IOException {
    }

    @Override
    public void sinkConfInit(FlowProcess<Config> flowProcess, Tap<Config, InputStream, Object> tap, Config properties) {
    }

    @Override
    public boolean source(FlowProcess<Config> flowProcess, SourceCall<Reader, InputStream> sourceCall) throws IOException {
        throw new UnsupportedOperationException("NullSinkTap can only be used as a sink, not a source");
    }

    @Override
    public void sourceConfInit(FlowProcess<Config> flowProcess, Tap<Config, InputStream, Object> tap, Config properties) {
        throw new UnsupportedOperationException("NullSinkTap can only be used as a sink, not a source");
    }
    
}