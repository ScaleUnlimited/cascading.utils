package com.bixolabs.cascading.local;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.bixolabs.cascading.NullContext;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class InMemoryTapLocalTest {

    @Test
    public void testWithLocalFlow() {
        final Fields sourceFields = new Fields("letter");
        InMemoryTap sourceTap = new InMemoryTap(sourceFields, new Tuple("a"), new Tuple("b"));
        
        final Fields sinkFields = sourceFields;
        InMemoryTap sinkTap = new InMemoryTap(sinkFields);
        
        Pipe p = new Pipe("pipe");
        Flow f = new LocalFlowConnector().connect(sourceTap, sinkTap, p);
        f.complete();
        
        List<TupleEntry> results = sinkTap.getOutput();
        assertEquals(2, results.size());
        assertEquals("a", results.get(0).getString("letter"));
        assertEquals("b", results.get(1).getString("letter"));
        
    }

}
