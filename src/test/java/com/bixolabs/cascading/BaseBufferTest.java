package com.bixolabs.cascading;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BufferCall;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class BaseBufferTest {

    @SuppressWarnings("serial")
    private static class MyBuffer extends BaseBuffer {

        private boolean _stopOnPrepareException;
        
        public MyBuffer(boolean stopOnPrepareException) {
            super(new Fields("key", "value"));
            
            _stopOnPrepareException = stopOnPrepareException;
        }
        
        @Override
        public void prepare() throws Exception {
            throw new IllegalStateException("Some exception");
        }
        
        @Override
        public void process(BufferCall<NullContext> bufferCall) throws Exception {
            Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
            if (iter.hasNext()) {
                emit(iter.next());
            }
        }
        
        @Override
        public boolean handlePrepareException(Throwable t) {
            return !_stopOnPrepareException;
        }
    }
    
    @Test
    public void testSimple() throws Exception {
        Fields testFields = new Fields("key", "value");
        
        File tmpInDir = new File("build/test/BaseBufferTest/testSimple/in");
        Lfs in = new Lfs(new SequenceFile(testFields), tmpInDir.getAbsolutePath(), true);
        
        TupleEntryCollector writer = in.openForWrite(new HadoopFlowProcess());
        writer.add(new Tuple("a", 2));
        writer.add(new Tuple("a", 1));
        writer.add(new Tuple("a", 1));
        writer.add(new Tuple("b", 5));
        writer.close();
        
        // Now create a flow that does an FirstBy
        Pipe pipe = new Pipe("pipe");
        pipe = new GroupBy(pipe, new Fields("key"), new Fields("value"));
        pipe = new Every(pipe, new MyBuffer(false), Fields.RESULTS);
        
        File tmpOutDir = new File("build/test/BaseBufferTest/testSimple/out");
        Lfs out = new Lfs(new SequenceFile(testFields), tmpOutDir.getAbsolutePath(), true);

        FlowConnector flowConnector = new HadoopFlowConnector();
        Flow flow = flowConnector.connect(in, out, pipe);
        flow.complete();
        
        TupleEntryIterator iter = out.openForRead(new HadoopFlowProcess());
        assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        assertEquals("a", te.getString("key"));
        assertEquals(1, te.getInteger("value"));
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("b", te.getString("key"));
        assertEquals(5, te.getInteger("value"));
        
        assertFalse(iter.hasNext());
    }
    
    @Test
    public void testExceptionInPrepare() throws Exception {
        Fields testFields = new Fields("key", "value");
        
        File tmpInDir = new File("build/test/BaseBufferTest/testExceptionInPrepare/in");
        Lfs in = new Lfs(new SequenceFile(testFields), tmpInDir.getAbsolutePath(), true);
        
        TupleEntryCollector writer = in.openForWrite(new HadoopFlowProcess());
        writer.add(new Tuple("a", 1));
        writer.close();
        
        // Now create a flow that does an FirstBy
        Pipe pipe = new Pipe("pipe");
        pipe = new GroupBy(pipe, new Fields("key"), new Fields("value"));
        pipe = new Every(pipe, new MyBuffer(true), Fields.RESULTS);
        
        File tmpOutDir = new File("build/test/BaseBufferTest/testExceptionInPrepare/out");
        Lfs out = new Lfs(new SequenceFile(testFields), tmpOutDir.getAbsolutePath(), true);

        FlowConnector flowConnector = new HadoopFlowConnector();
        Flow flow = flowConnector.connect(in, out, pipe);
        
        try {
            flow.complete();
            fail("Flow should have terminated with exception");
        } catch (Exception e) {
            // valid
        }
    }

}
