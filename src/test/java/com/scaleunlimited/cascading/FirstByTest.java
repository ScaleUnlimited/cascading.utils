package com.scaleunlimited.cascading;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.scaleunlimited.cascading.FirstBy;
import com.scaleunlimited.cascading.local.DirectoryTap;
import com.scaleunlimited.cascading.local.KryoScheme;

import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy.CompositeFunction;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;


public class FirstByTest {

    @Test
    public void testFindingFirst() throws IOException {
        Fields testFields = new Fields("grouping", "value", "data");
        Lfs in = makeInputData("build/test/FirstByTest/testFindingFirst/in", testFields, false);
        
        // Now create a flow that does an FirstBy
        Pipe pipe = new Pipe("aggregate");
        pipe = new FirstBy(pipe, new Fields("grouping"), new Fields("value"));
        
        File tmpOutDir = new File("build/test/FirstByTest/testFindingFirst/out");
        Lfs out = new Lfs(new SequenceFile(testFields), tmpOutDir.getAbsolutePath(), true);

        FlowConnector flowConnector = new HadoopFlowConnector();
        flowConnector.connect(in, out, pipe).complete();
        
        TupleEntryIterator iter = out.openForRead(new HadoopFlowProcess());
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
    public void testReverseSort() throws Exception {
        Fields testFields = new Fields("grouping", "value", "data");
        Lfs in = makeInputData("build/test/FirstByTest/testReverseSort/in", testFields, true);
        
        // Now create a flow that does an FirstBy. We sort by value (reverse order), and the
        // only data we want to output is the data field.
        Pipe pipe = new Pipe("aggregate");
        pipe = new FirstBy("reverse sort", pipe, new Fields("grouping"), new Fields("value"), true, new Fields("data"));
        
        File tmpOutDir = new File("build/test/FirstByTest/testReverseSort/out");
        Lfs out = new Lfs(new SequenceFile(new Fields("data")), tmpOutDir.getAbsolutePath(), true);

        FlowConnector flowConnector = new HadoopFlowConnector();
        flowConnector.connect(in, out, pipe).complete();
        
        TupleEntryIterator iter = out.openForRead(new HadoopFlowProcess());
        assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        assertEquals("b-extra", te.getString("data"));
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("a-extra1", te.getString("data"));
        
        assertFalse(iter.hasNext());
    }
    
    @Test
    public void testReverseSortLocal() throws Exception {
        Fields testFields = new Fields("grouping", "value", "data");
        DirectoryTap in1 = makeInputDataLocalLeft("build/test/FirstByTest/testReverseSort/left", testFields);
        DirectoryTap in2 = makeInputDataLocalRight("build/test/FirstByTest/testReverseSort/right", testFields);
        
        // Now create a flow that does an FirstBy. We sort by value (reverse order), and the
        // only data we want to output is the data field.
        Pipe pipe1 = new Pipe("pipe1");
        Pipe pipe2 = new Pipe("pipe2");
        Pipe aggregate = new FirstBy("reverse sort", Pipe.pipes(pipe1, pipe2), new Fields("grouping"), new Fields("value"), true, Fields.ALL, CompositeFunction.DEFAULT_THRESHOLD);
        
        File tmpOutDir = new File("build/test/FirstByTest/testReverseSort/out");
        DirectoryTap out = new DirectoryTap(new KryoScheme(testFields), tmpOutDir.getAbsolutePath(), SinkMode.REPLACE);

        Map<String, Tap> sources = new HashMap<String, Tap>();
        sources.put(pipe1.getName(), in1);
        sources.put(pipe2.getName(), in2);
        
        FlowConnector flowConnector = new LocalFlowConnector();
        flowConnector.connect(sources, out, aggregate).complete();
        
        TupleEntryIterator iter = out.openForRead(new LocalFlowProcess());
        assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        assertEquals("b-extra", te.getString("data"));
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("a-extra1", te.getString("data"));
        
        assertFalse(iter.hasNext());
    }
    

    private Lfs makeInputData(String inputDir, Fields testFields, boolean reversed) throws IOException {
        File tmpInDir = new File(inputDir);
        Lfs in = new Lfs(new SequenceFile(testFields), tmpInDir.getAbsolutePath(), true);
        
        TupleEntryCollector writer = in.openForWrite(new HadoopFlowProcess());
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

    private DirectoryTap makeInputDataLocalLeft(String inputDir, Fields testFields) throws IOException {
        File tmpInDir = new File(inputDir);
        DirectoryTap in = new DirectoryTap(new KryoScheme(testFields), tmpInDir.getAbsolutePath(), SinkMode.REPLACE);
        
        TupleEntryCollector writer = in.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("b", 5, "b-extra"));
        writer.close();
        
        return in;
    }

    private DirectoryTap makeInputDataLocalRight(String inputDir, Fields testFields) throws IOException {
        File tmpInDir = new File(inputDir);
        DirectoryTap in = new DirectoryTap(new KryoScheme(testFields), tmpInDir.getAbsolutePath(), SinkMode.REPLACE);
        
        TupleEntryCollector writer = in.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("a", 1, "a-extra2"));
        writer.add(new Tuple("a", 2, "a-extra1"));
        writer.close();
        
        return in;
    }

}
