package com.scaleunlimited.cascading;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import com.scaleunlimited.cascading.GroupLimit;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
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

public class GroupLimitTest {

    @Test
    public void test() throws Exception {
        final Fields groupField = new Fields("user");
        final Fields sortField = new Fields("value");
        final Fields testFields = new Fields("user", "value");
        
        String in = "build/test/GroupLimitTest/test/in";
        String out = "build/test/GroupLimitTest/test/out";

        Lfs sourceTap = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
        TupleEntryCollector write = sourceTap.openForWrite(new HadoopFlowProcess());
        write.add(new Tuple("user1", 1));
        write.add(new Tuple("user1", 2));
        write.add(new Tuple("user2", 1));
        write.add(new Tuple("user2", 2));
        write.add(new Tuple("user2", 3));
        write.close();

        Pipe pipe = new Pipe("test");
        pipe = new GroupBy(pipe, groupField, sortField, true);
        pipe = new Every(pipe, new GroupLimit(1), Fields.RESULTS);
        
        Lfs sinkTap = new Lfs(new SequenceFile(testFields), out, SinkMode.REPLACE);
        
        Flow flow = new HadoopFlowConnector().connect(sourceTap, sinkTap, pipe);
        flow.complete();
        
        TupleEntryIterator iter = sinkTap.openForRead(new HadoopFlowProcess());

        TupleEntry te = iter.next();
        assertEquals("user2", te.getString("user"));
        assertEquals(3, te.getInteger("value"));
        
        te = iter.next();
        assertEquals("user1", te.getString("user"));
        assertEquals(2, te.getInteger("value"));
        
        assertFalse(iter.hasNext());
    }

}
