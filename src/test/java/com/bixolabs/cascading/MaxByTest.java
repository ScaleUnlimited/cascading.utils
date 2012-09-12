package com.bixolabs.cascading;


import java.io.File;
import java.io.IOException;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class MaxByTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testMaxByAndInjectionOfField() throws IOException {
        File tmpDirLHS = new File("build/test/MaxByTest/testMaxByAndInjectionOfField/in");
        Fields inFields = new Fields("grouping", "summing", "maxing");
        Lfs in = new Lfs(new SequenceFile(inFields), tmpDirLHS.getAbsolutePath(), true);
        TupleEntryCollector writer = in.openForWrite(new HadoopFlowProcess());
        
        writer.add(new Tuple("a", 2, 11));
        writer.add(new Tuple("a", 1, 12));
        writer.add(new Tuple("a", 3, 5));
        writer.add(new Tuple("b", 5, 13));
        writer.close();
        
        // Now create a flow that does an AggregateBy, where we're also summing
        Pipe pipe = new Pipe("aggregate");
        pipe = new AggregateBy( pipe,
                                new Fields("grouping"),
                                new MaxBy(new Fields("maxing"), new Fields("max"), Integer.class),
                                new SumBy(new Fields("summing"), new Fields("sum"), Integer.class));
        // pipe = new Each(pipe, new Debug("aggregated", true));
        
        File outDir = new File("build/test/MaxByTest/testMaxByAndInjectionOfField/out");
        Fields outFields = new Fields("grouping", "max", "sum");
        Lfs out = new Lfs(new SequenceFile(outFields), outDir.getAbsolutePath(), true);

        HadoopFlowConnector flowConnector = new HadoopFlowConnector();
        Flow flow = flowConnector.connect(in, out, pipe);
        flow.complete();
        
        // Verify we get the right results.
        TupleEntryIterator iter = out.openForRead(new HadoopFlowProcess());
        
        Assert.assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        Assert.assertEquals("a", te.getString("grouping"));
        Assert.assertEquals(12, te.getInteger("max"));
        Assert.assertEquals(6, te.getInteger("sum"));
        
        Assert.assertTrue(iter.hasNext());
        te = iter.next();
        Assert.assertEquals("b", te.getString("grouping"));
        Assert.assertEquals(13, te.getInteger("max"));
        Assert.assertEquals(5, te.getInteger("sum"));
        
        Assert.assertFalse(iter.hasNext());
    }
    
}
