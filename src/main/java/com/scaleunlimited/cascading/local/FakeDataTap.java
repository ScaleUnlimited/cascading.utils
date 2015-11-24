package com.scaleunlimited.cascading.local;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.SourceTap;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeIterator;

@SuppressWarnings("serial")
public class FakeDataTap extends SourceTap<Properties, InputStream> {

    private static class FakeDataScheme extends Scheme<Properties, InputStream, Object, AtomicInteger, Object> {

        private int _numResults;
        
        // TODO instead of passing in numResults, give it an iFakeDataGenerator class. This must be serializable,
        // and would have been constructed with the numResults limit
        
        public FakeDataScheme(int numResults) {
            super(new Fields("line"));
            
            _numResults = numResults;
        }
        
        @Override
        public void sourceConfInit(FlowProcess<? extends Properties> flowProcess, Tap<Properties, InputStream, Object> tap, Properties conf) {
            // TODO I think I need to stash the numResults value in the conf, and use it
            // in the sourcePrepare call to set up the context.
            conf.setProperty("numresults", "" + _numResults);
            
        }

        @Override
        public void sinkConfInit(FlowProcess<? extends Properties> flowProcess, Tap<Properties, InputStream, Object> tap, Properties conf) {
            throw new TapException("FakeDataTap can only be used as a source, not a sink");
        }

        @Override
        public void sourcePrepare(FlowProcess<? extends Properties> flowProcess, SourceCall<AtomicInteger, InputStream> sourceCall) throws IOException {
            super.sourcePrepare(flowProcess, sourceCall);
            
            
            // TODO - set up numResults from the configuration somehow.
        }
        
        @Override
        public boolean source(FlowProcess<? extends Properties> flowProcess, SourceCall<AtomicInteger, InputStream> sourceCall) throws IOException {
            // TODO Auto-generated method stub
            sourceCall.getIncomingEntry().setTuple(null);

            return false;
        }

        @Override
        public void sink(FlowProcess<? extends Properties> flowProcess, SinkCall<Object, Object> sinkCall) throws IOException {
            throw new TapException("FakeDataTap can only be used as a source, not a sink");
        }
    }
    
    public FakeDataTap(int numResults) {
        super(new FakeDataScheme(numResults));
    }

    @Override
    public String getIdentifier() {
        return "FakeDataTap";
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<? extends Properties> flowProcess, InputStream input) throws IOException {
        return new TupleEntrySchemeIterator<Properties, InputStream>(flowProcess, getScheme(), input);
    }

    @Override
    public boolean resourceExists(Properties conf) throws IOException {
        return true;
    }

    @Override
    public long getModifiedTime(Properties conf) throws IOException {
        return System.currentTimeMillis();
    }

}
