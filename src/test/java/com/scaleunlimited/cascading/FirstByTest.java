package com.scaleunlimited.cascading;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.scaleunlimited.cascading.FirstBy;

import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
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
        Lfs in = makeInputData("build/test/FirstByTest/testFindingFirst/in", testFields);
        
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
        Lfs in = makeInputData("build/test/FirstByTest/testReverseSort/in", testFields);
        
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
    
    private Lfs makeInputData(String inputDir, Fields testFields) throws IOException {
        File tmpInDir = new File("build/test/FirstByTest/testFindingFirst/in");
        Lfs in = new Lfs(new SequenceFile(testFields), tmpInDir.getAbsolutePath(), true);
        
        TupleEntryCollector writer = in.openForWrite(new HadoopFlowProcess());
        writer.add(new Tuple("a", 2, "a-extra1"));
        writer.add(new Tuple("a", 1, "a-extra2"));
        writer.add(new Tuple("a", 1, "a-extra4"));
        writer.add(new Tuple("b", 5, "b-extra"));
        writer.close();
        
        return in;
    }

}
