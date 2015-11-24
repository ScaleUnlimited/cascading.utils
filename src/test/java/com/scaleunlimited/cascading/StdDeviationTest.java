package com.scaleunlimited.cascading;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class StdDeviationTest {

    @Test
    public void test() throws Exception {
        // Set up input tuples that have two groups, each with 10K values.
        
        final Fields groupField = new Fields("user");
        final Fields testFields = new Fields("user", "value");
        
        String in = "build/test/StatisticsTest/test/in";
        String out = "build/test/GroupLimitTest/test/out";

        Lfs sourceTap = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
        TupleEntryCollector write = sourceTap.openForWrite(new HadoopFlowProcess());
        
        Random gen = new Random(1L);
        for (int i = 0; i < 10000; i++) {
            write.add(new Tuple("user1", gen.nextGaussian()));
        }
        
        gen = new Random(1L);
        for (int i = 0; i < 10000; i++) {
            write.add(new Tuple("user2", gen.nextGaussian()));
        }
        write.close();

        Pipe pipe = new Pipe("test");
        pipe = new GroupBy(pipe, groupField);
        pipe = new Every(pipe, new Fields("value"), new StdDeviation(), Fields.ALL);
        
        Lfs sinkTap = new Lfs(new SequenceFile(new Fields("user", StdDeviation.FIELD_NAME)), out, SinkMode.REPLACE);
        
        Flow flow = new Hadoop2MR1FlowConnector().connect(sourceTap, sinkTap, pipe);
        flow.complete();
        
        TupleEntryIterator iter = sinkTap.openForRead(new HadoopFlowProcess());

        TupleEntry te = iter.next();
        assertEquals("user1", te.getString("user"));
        double stdDeviation = te.getDouble(StdDeviation.FIELD_NAME);
        assertTrue(stdDeviation >= 0.988395);
        assertTrue(stdDeviation <= 1.011883);
        
        
        te = iter.next();
        assertEquals("user2", te.getString("user"));
        stdDeviation = te.getDouble(StdDeviation.FIELD_NAME);
        assertTrue(stdDeviation >= 0.988395);
        assertTrue(stdDeviation <= 1.011883);
        
        assertFalse(iter.hasNext());
    
    }

}
