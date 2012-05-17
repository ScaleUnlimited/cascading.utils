package com.bixolabs.cascading;

import static org.junit.Assert.*;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Debug;
import cascading.operation.filter.Limit;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
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
        TupleEntryCollector write = sourceTap.openForWrite(new JobConf());
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
        
        Flow flow = new FlowConnector().connect(sourceTap, sinkTap, pipe);
        flow.complete();
        
        TupleEntryIterator iter = sinkTap.openForRead(new JobConf());

        TupleEntry te = iter.next();
        assertEquals("user2", te.getString("user"));
        assertEquals(3, te.getInteger("value"));
        
        te = iter.next();
        assertEquals("user1", te.getString("user"));
        assertEquals(2, te.getInteger("value"));
        
        assertFalse(iter.hasNext());
    }

}
