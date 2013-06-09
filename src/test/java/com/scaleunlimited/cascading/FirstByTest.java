package com.scaleunlimited.cascading;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy.CompositeFunction;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.hadoop.HadoopPlatform;
import com.scaleunlimited.cascading.local.LocalPlatform;


public class FirstByTest {

    @Test
    public void testFindingFirst() throws Exception {
        BasePlatform platform = new LocalPlatform(FirstByTest.class);
        Fields testFields = new Fields("grouping", "value", "data");
        Tap in = makeInputData(platform, "build/test/FirstByTest/testFindingFirst/in", testFields, false);
        
        // Now create a flow that does an FirstBy
        Pipe pipe = new Pipe("aggregate");
        pipe = new FirstBy(pipe, new Fields("grouping"), new Fields("value"));
        
        BasePath tmpOutDir = platform.makePath("build/test/FirstByTest/testFindingFirst/out");
        Tap out = platform.makeTap(platform.makeBinaryScheme(testFields), tmpOutDir, SinkMode.REPLACE);

        FlowConnector flowConnector = platform.makeFlowConnector();
        flowConnector.connect(in, out, pipe).complete();
        
        TupleEntryIterator iter = out.openForRead(platform.makeFlowProcess());
        assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        assertEquals("a", te.getString("grouping"));
        assertEquals(1, te.getInteger("value"));
        assertEquals("a-extra2", te.getString("data"));
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("b", te.getString("grouping"));
        assertEquals(5, te.getInteger("value"));
        assertEquals("b-extra", te.getString("data"));
        
        assertFalse(iter.hasNext());
    }
    
    @Test
    public void testReverseSortHadoop() throws Exception {
        testReverseSort(new HadoopPlatform(FirstByTest.class));
    }
    
    @Test
    public void testReverseSortLocal() throws Exception {
        testReverseSort(new LocalPlatform(FirstByTest.class));
    }
    
    private void testReverseSort(BasePlatform platform) throws Exception {
        Fields testFields = new Fields("grouping", "value", "data");
        Tap in1 = makeInputDataLeft(platform, "build/test/FirstByTest/testReverseSort/left", testFields);
        Tap in2 = makeInputDataRight(platform, "build/test/FirstByTest/testReverseSort/right", testFields);
        
        // Now create a flow that does an FirstBy. We sort by value (reverse order), and the
        // only data we want to output is the data field.
        Pipe pipe1 = new Pipe("pipe1");
        Pipe pipe2 = new Pipe("pipe2");
        Pipe aggregate = new FirstBy("reverse sort", Pipe.pipes(pipe1, pipe2), new Fields("grouping"), new Fields("value"), true, Fields.ALL, CompositeFunction.DEFAULT_THRESHOLD);
        
        BasePath tmpOutDir = platform.makePath("build/test/FirstByTest/testReverseSort/out");
        Tap out = platform.makeTap(platform.makeBinaryScheme(testFields), tmpOutDir, SinkMode.REPLACE);

        Map<String, Tap> sources = new HashMap<String, Tap>();
        sources.put(pipe1.getName(), in1);
        sources.put(pipe2.getName(), in2);
        
        FlowConnector flowConnector = platform.makeFlowConnector();
        flowConnector.connect(sources, out, aggregate).complete();
        
        TupleEntryIterator iter = out.openForRead(platform.makeFlowProcess());
        assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        assertEquals("b-extra", te.getString("data"));
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("a-extra1", te.getString("data"));
        
        assertFalse(iter.hasNext());
    }
    
    private Tap makeInputData(BasePlatform platform, String inputDir, Fields testFields, boolean reversed) throws Exception {
        BasePath tmpInDir = platform.makePath(inputDir);
        Tap in = platform.makeTap(platform.makeBinaryScheme(testFields), tmpInDir, SinkMode.REPLACE);
        
        TupleEntryCollector writer = in.openForWrite(platform.makeFlowProcess());
        if (!reversed) {
            writer.add(new Tuple("a", 2, "a-extra1"));
        }
        
        writer.add(new Tuple("a", 1, "a-extra2"));
        writer.add(new Tuple("a", 1, "a-extra4"));
        
        if (reversed) {
            writer.add(new Tuple("a", 2, "a-extra1"));
        }
        writer.add(new Tuple("b", 5, "b-extra"));
        writer.close();
        
        return in;
    }

    private Tap makeInputDataLeft(BasePlatform platform, String inputDir, Fields testFields) throws Exception {
        BasePath tmpInDir = platform.makePath(inputDir);
        Tap in = platform.makeTap(platform.makeBinaryScheme(testFields), tmpInDir, SinkMode.REPLACE);
        
        TupleEntryCollector writer = in.openForWrite(platform.makeFlowProcess());
        writer.add(new Tuple("b", 5, "b-extra"));
        writer.close();
        
        return in;
    }

    private Tap makeInputDataRight(BasePlatform platform, String inputDir, Fields testFields) throws Exception {
        BasePath tmpInDir = platform.makePath(inputDir);
        Tap in = platform.makeTap(platform.makeBinaryScheme(testFields), tmpInDir, SinkMode.REPLACE);
        
        TupleEntryCollector writer = in.openForWrite(platform.makeFlowProcess());
        writer.add(new Tuple("a", 1, "a-extra2"));
        writer.add(new Tuple("a", 2, "a-extra1"));
        writer.close();
        
        return in;
    }

}
