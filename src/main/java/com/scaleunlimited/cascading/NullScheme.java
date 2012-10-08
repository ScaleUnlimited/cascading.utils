package com.scaleunlimited.cascading;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;

@SuppressWarnings("serial")
class NullScheme extends Scheme<Object, Object, Object, Object, Object> {
    
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
    public void sink(FlowProcess<Object> flowProcess, SinkCall<Object, Object> sinkCall) throws IOException {
    }

    @Override
    public void sinkConfInit(FlowProcess<Object> flowProcess, Tap<Object, Object, Object> tap, Object properties) {
    }

    @Override
    public boolean source(FlowProcess<Object> flowProcess, SourceCall<Object, Object> sourceCall) throws IOException {
        throw new UnsupportedOperationException("NullSinkTap can only be used as a sink, not a source");
    }

    @Override
    public void sourceConfInit(FlowProcess<Object> flowProcess, Tap<Object, Object, Object> tap, Object properties) {
        throw new UnsupportedOperationException("NullSinkTap can only be used as a sink, not a source");
    }
    
}