package com.scaleunlimited.cascading;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.BufferCall;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.local.LocalPlatform;

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
        
        BasePlatform platform = new LocalPlatform(BaseBufferTest.class);
        BasePath tmpInDir = platform.makePath("build/test/BaseBufferTest/testSimple/in");
        Tap in = platform.makeTap(platform.makeBinaryScheme(testFields), tmpInDir, SinkMode.REPLACE);
        
        TupleEntryCollector writer = in.openForWrite(platform.makeFlowProcess());
        writer.add(new Tuple("a", 2));
        writer.add(new Tuple("a", 1));
        writer.add(new Tuple("a", 1));
        writer.add(new Tuple("b", 5));
        writer.close();
        
        // Now create a flow that does an FirstBy
        Pipe pipe = new Pipe("pipe");
        pipe = new GroupBy(pipe, new Fields("key"), new Fields("value"));
        pipe = new Every(pipe, new MyBuffer(false), Fields.RESULTS);
        
        BasePath tmpOutDir = platform.makePath("build/test/BaseBufferTest/testSimple/out");
        Tap out = platform.makeTap(platform.makeBinaryScheme(testFields), tmpOutDir, SinkMode.REPLACE);

        FlowConnector flowConnector = platform.makeFlowConnector();
        Flow flow = flowConnector.connect(in, out, pipe);
        flow.complete();
        
        TupleEntryIterator iter = out.openForRead(platform.makeFlowProcess());
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
        
        BasePlatform platform = new LocalPlatform(BaseBufferTest.class);
        BasePath tmpInDir = platform.makePath("build/test/BaseBufferTest/testExceptionInPrepare/in");
        Tap in = platform.makeTap(platform.makeBinaryScheme(testFields), tmpInDir, SinkMode.REPLACE);

        TupleEntryCollector writer = in.openForWrite(platform.makeFlowProcess());
        writer.add(new Tuple("a", 1));
        writer.close();
        
        // Now create a flow that does an FirstBy
        Pipe pipe = new Pipe("pipe");
        pipe = new GroupBy(pipe, new Fields("key"), new Fields("value"));
        pipe = new Every(pipe, new MyBuffer(true), Fields.RESULTS);
        
        BasePath tmpOutDir = platform.makePath("build/test/BaseBufferTest/testExceptionInPrepare/out");
        Tap out = platform.makeTap(platform.makeBinaryScheme(testFields), tmpOutDir, SinkMode.REPLACE);

        FlowConnector flowConnector = platform.makeFlowConnector();
        Flow flow = flowConnector.connect(in, out, pipe);
        
        try {
            flow.complete();
            fail("Flow should have terminated with exception");
        } catch (Exception e) {
            // valid
        }
    }

}
