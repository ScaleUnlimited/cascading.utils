package com.scaleunlimited.cascading.local;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * This is a simple binary format scheme, for when a Cascading local flow wants to mimic what a Hadoop flow
 * does via a SequenceFile.
 * 
 */
@SuppressWarnings("serial")
public class KryoScheme extends Scheme<Properties, InputStream, OutputStream, KryoContext, KryoContext> {
    private static final Logger LOGGER = Logger.getLogger(KryoScheme.class);
    
    public KryoScheme(Fields fields) {
        super(fields);
    }
    
    @Override
    public void sourceConfInit(FlowProcess<Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void sinkConfInit(FlowProcess<Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf) {
        LOGGER.trace("KryoScheme - sinkConfInit");
    }

    @Override
    public void presentSinkFields(FlowProcess<Properties> flowProcess, Tap tap, Fields fields) {
        super.presentSinkFields(flowProcess, tap, fields);
    }
    
    @Override
    public void sourcePrepare(FlowProcess<Properties> flowProcess, SourceCall<KryoContext, InputStream> sourceCall) throws IOException {
        super.sourcePrepare(flowProcess, sourceCall);
        
        sourceCall.setContext(new KryoContext(new Input(sourceCall.getInput()), getSourceFields()));
    }
    
    @Override
    public boolean source(FlowProcess<Properties> flowProcess, SourceCall<KryoContext, InputStream> sourceCall) throws IOException {
        // TODO select source fields data from tuple
        Tuple t = sourceCall.getContext().deserialize();
        if (t != null) {
            sourceCall.getIncomingEntry().setTuple(t);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void sinkPrepare(FlowProcess<Properties> flowProcess, SinkCall<KryoContext, OutputStream> sinkCall) throws IOException {
        super.sinkPrepare(flowProcess, sinkCall);
        
        KryoContext context = new KryoContext(new Output(sinkCall.getOutput()), getSinkFields());
        sinkCall.setContext(context);
    }
    
    @Override
    public void sink(FlowProcess<Properties> flowProcess, SinkCall<KryoContext, OutputStream> sinkCall) throws IOException {
        sinkCall.getContext().serialize(sinkCall.getOutgoingEntry().getTuple());
    }
    
    @Override
    public void sinkCleanup(FlowProcess<Properties> flowProcess, SinkCall<KryoContext, OutputStream> sinkCall) throws IOException {
        // Write a null object as an end of input marker
        // TODO - figure out if this is the best approach
        sinkCall.getContext().serialize(null);
        sinkCall.getContext().close();
        
        super.sinkCleanup(flowProcess, sinkCall);
    }

}
