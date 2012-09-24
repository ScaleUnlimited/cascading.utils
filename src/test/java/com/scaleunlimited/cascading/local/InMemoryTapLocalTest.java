package com.scaleunlimited.cascading.local;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.scaleunlimited.cascading.NullContext;
import com.scaleunlimited.cascading.local.InMemoryTap;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

public class InMemoryTapLocalTest {

    @Test
    public void testWithLocalFlow() throws Exception {
        final Fields sourceFields = new Fields("letter");
        InMemoryTap sourceTap = new InMemoryTap(sourceFields, sourceFields, SinkMode.REPLACE);
        TupleEntryCollector writer = sourceTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("a"));
        writer.add(new Tuple("b"));
        writer.close();
        
        final Fields sinkFields = sourceFields;
        InMemoryTap sinkTap = new InMemoryTap(sinkFields, sinkFields, SinkMode.REPLACE);
        
        Pipe p = new Pipe("pipe");
        Flow f = new LocalFlowConnector().connect(sourceTap, sinkTap, p);
        f.complete();
        
        List<TupleEntry> results = sinkTap.getOutput();
        assertEquals(2, results.size());
        assertEquals("a", results.get(0).getString("letter"));
        assertEquals("b", results.get(1).getString("letter"));
        
    }

}
