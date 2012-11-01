package com.scaleunlimited.cascading;


import java.io.IOException;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.MaxBy;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class MaxByTest {

    @Test
    public void testMaxByAndInjectionOfField() throws IOException {
        Fields inFields = new Fields("grouping", "summing", "maxing");
        InMemoryTap in = new InMemoryTap(inFields, inFields, SinkMode.REPLACE);
        TupleEntryCollector writer = in.openForWrite(new LocalFlowProcess());
        
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
        
        Fields outFields = new Fields("grouping", "max", "sum");
        InMemoryTap out = new InMemoryTap(outFields, outFields, SinkMode.REPLACE);

        LocalFlowConnector flowConnector = new LocalFlowConnector();
        Flow<Properties> flow = flowConnector.connect(in, out, pipe);
        flow.complete();
        
        // Verify we get the right results.
        TupleEntryIterator iter = out.openForRead(new LocalFlowProcess());
        
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
