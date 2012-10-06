package com.scaleunlimited.cascading;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import com.scaleunlimited.cascading.FlowCounters;
import com.scaleunlimited.cascading.NullContext;
import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.DirectoryTap;
import com.scaleunlimited.cascading.local.KryoScheme;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.state.Counter;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.SumBy;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;


public class FlowCountersTest {

    private enum FlowCountersTestEnum {
        TUPLE_COUNT,
        UNUSED_COUNT,
        BOGUS_COUNT,
        
        PRE_BREAK_COUNT,
        POST_BREAK_COUNT,
        LEFT_COUNT,
        RIGHT_COUNT
    }
    
    @SuppressWarnings("serial")
    private static class CountTuplesFunction extends BaseOperation<NullContext> implements Filter<NullContext> {
        
        @Override
        public boolean isRemove(FlowProcess flowProcess, FilterCall<NullContext> filterCall) {
            flowProcess.increment(FlowCountersTestEnum.TUPLE_COUNT, 1);
            return false;
        }
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void testCounters() throws Throwable {
        final Fields testFields = new Fields("user", "value");
        
        final int numDatums = 1;
        
        final String testDir = "build/test/FlowCountersTest/testCounters/";
        String in = testDir + "in";

        Lfs sourceTap = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
        TupleEntryCollector write = sourceTap.openForWrite(new HadoopFlowProcess());
        
        for (int i = 0; i < numDatums; i++) {
            String username = "user-" + (i % 3);
            write.add(new Tuple(username, i));
        }
        
        write.close();

        Pipe pipe = new Pipe("test");
        pipe = new Each(pipe, new CountTuplesFunction());
        Tap sinkTap = new NullSinkTap(testFields);
        
        Flow flow = new HadoopFlowConnector().connect(sourceTap, sinkTap, pipe);
        Map<Enum, Long> counters = FlowCounters.run(flow, FlowCountersTestEnum.TUPLE_COUNT,
                        FlowCountersTestEnum.UNUSED_COUNT);
        
        Assert.assertEquals(numDatums, (long)counters.get(FlowCountersTestEnum.TUPLE_COUNT));
        Assert.assertEquals(0, (long)counters.get(FlowCountersTestEnum.UNUSED_COUNT));
        Assert.assertNull(counters.get(FlowCountersTestEnum.BOGUS_COUNT));
    }
    
    @Test
    public void testCountersWithLocalMode() throws Exception {
        
        // We want to create a Flow with two tail pipes, and have each of the
        // tail pipes set counters that we'll check.
        final Fields testFields = new Fields("user", "value");
        
        final int numDatums = 8;
        
        final String testDir = "build/test/FlowCountersTest/testCountersWithLocalMode/";
        String in = testDir + "in";

        DirectoryTap sourceTap = new DirectoryTap(new KryoScheme(testFields), in, SinkMode.REPLACE);
        TupleEntryCollector write = sourceTap.openForWrite(new LocalFlowProcess());
        
        for (int i = 0; i < numDatums; i++) {
            String username = "user-" + (i % 2);
            write.add(new Tuple(username, i));
        }
        
        write.close();

        Pipe headPipe = new Pipe("head");
        headPipe = new Each(headPipe, new Counter(FlowCountersTestEnum.PRE_BREAK_COUNT));
        headPipe = new Each(headPipe, new Counter(FlowCountersTestEnum.POST_BREAK_COUNT));
        
        Pipe leftPipe = new Pipe("left", headPipe);
        leftPipe = new Each(leftPipe, new Fields("value"), new ExpressionFilter("value != 0", Integer.class));
        leftPipe = new Each(leftPipe, new Counter(FlowCountersTestEnum.LEFT_COUNT));
        
        Pipe rightPipe = new Pipe("right", headPipe);
        rightPipe = new Each(rightPipe, new Fields("value"), new ExpressionFilter("value == 0", Integer.class));
        rightPipe = new SumBy(rightPipe, new Fields("user"), new Fields("value"), new Fields("sum"), Integer.class);
        // We have two different users, so that's how many unique user name+sum values we should be getting.
        rightPipe = new Each(rightPipe, new Counter(FlowCountersTestEnum.RIGHT_COUNT));
        
        Map<String, Tap> sinks = new HashMap<String, Tap>();
        sinks.put(leftPipe.getName(), new NullSinkTap());
        sinks.put(rightPipe.getName(), new NullSinkTap());
        
        Flow flow = new LocalFlowConnector().connect(sourceTap, sinks, leftPipe, rightPipe);
        // flow.writeDOT(testDir + "flow.dot");
        // This fails with "more than one sink" error message.
        // flow.writeStepsDOT(testDir + "flowstep.dot");
        
        Map<Enum, Long> counters = FlowCounters.run(flow, FlowCountersTestEnum.PRE_BREAK_COUNT, FlowCountersTestEnum.POST_BREAK_COUNT,
                        FlowCountersTestEnum.LEFT_COUNT, FlowCountersTestEnum.RIGHT_COUNT);
        
        Assert.assertEquals(numDatums, (long)counters.get(FlowCountersTestEnum.PRE_BREAK_COUNT));
        Assert.assertEquals(numDatums, (long)counters.get(FlowCountersTestEnum.POST_BREAK_COUNT));
        Assert.assertEquals(1, (long)counters.get(FlowCountersTestEnum.LEFT_COUNT));
        Assert.assertEquals(2, (long)counters.get(FlowCountersTestEnum.RIGHT_COUNT));
    }
}
