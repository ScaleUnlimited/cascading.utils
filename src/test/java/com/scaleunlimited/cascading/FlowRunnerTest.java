package com.scaleunlimited.cascading;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class FlowRunnerTest {

    private enum MyCounters {
        FILTER_REQUESTS,
    }
    
    @SuppressWarnings({ "serial", "rawtypes" })
    private static class MyFilter extends BaseOperation implements Filter {

        private boolean _fails;
        
        public MyFilter(boolean fails) {
            _fails = fails;
        }
        
        @Override
        public boolean isRemove(FlowProcess process, FilterCall filterCall) {
            if (_fails) {
                throw new RuntimeException("We failed!");
            }
            
            process.increment(MyCounters.FILTER_REQUESTS, 1);
            return false;
        }
    }
    
    @Test
    public void testAsyncOperation() throws Throwable {
        FlowRunner fr = new FlowRunner();
        Assert.assertTrue(fr.isDone());
        
        FlowFuture result0 = fr.addFlow(makeFlow(10, 0));
        FlowFuture result1 = fr.addFlow(makeFlow(100, 1));
        Assert.assertFalse(fr.isDone());

        // Try the get() call on the future before it will have completed.
        Map<String, Long> counters0 = result0.get().getCounters();
        Assert.assertEquals(10, (long)counters0.get(MyCounters.class.getName() + "." + MyCounters.FILTER_REQUESTS.name()));
        
        // Now wait for everything to complete.
        fr.complete();
        
        Map<String, Long> counters1 = result1.get().getCounters();
        Assert.assertEquals(100, (long)counters1.get(MyCounters.class.getName() + "." + MyCounters.FILTER_REQUESTS.name()));
    }
    
    @Test
    public void testShortWait() throws Exception {
        FlowRunner fr = new FlowRunner();
        FlowFuture result = fr.addFlow(makeFlow(100, 0));
        
        try {
            // Wait for a very short amount of time.
            result.get(1, TimeUnit.NANOSECONDS);
            Assert.fail("No TimeoutException was thrown");
        } catch (TimeoutException e) {
            // what we want
        }
    }
    
    @Test
    public void testFailureHandling() throws Exception {
        FlowRunner fr = new FlowRunner();
        FlowFuture result = fr.addFlow(makeFlow(100, 0, true));

        try {
            result.get();
            Assert.fail("No ExecutionException was thrown");
        } catch (ExecutionException e) {
            // what we want
        }

    }
    
    @Test
    public void testCancelling() throws Exception {
        FlowRunner fr = new FlowRunner();
        FlowFuture result = fr.addFlow(makeFlow(100, 0, true));
        
        // Have to interrupt running job to get it to be canceled
        Assert.assertFalse(result.isDone());
        Assert.assertFalse(result.isCancelled());
        Assert.assertFalse(result.cancel(false));
        Assert.assertFalse(result.isDone());
        Assert.assertFalse(result.isCancelled());

        // Really cancel it.
        Assert.assertTrue(result.cancel(true));
        Assert.assertTrue(result.isDone());
        Assert.assertTrue(result.isCancelled());

        try {
            result.get();
            Assert.fail("No CancellationException was thrown");
        } catch (CancellationException e) {
            // what we want
        }

    }
    
    @Test
    public void testIsFull() throws Throwable {
        
        // TODO It would be better to test with a larger capacity, but it only
        // runs one flow at a time in local mode.
        
        // An empty runner shouldn't be full.
        FlowRunner fr = new FlowRunner(1);
        Assert.assertFalse(fr.isFull());
        
        // There should be no room after we fill it up.
        FlowFuture result0 = fr.addFlow(makeFlow(10, 0));
        Assert.assertTrue(fr.isFull());
        
        // There should be room after the first flow finishes.
        result0.get();
        Assert.assertFalse(fr.isFull());

        // There should be no room after we fill the empty slot.
        fr.addFlow(makeFlow(10, 1));
        Assert.assertTrue(fr.isFull());

        // There should be room after everything completes.
        fr.complete();
        Assert.assertFalse(fr.isFull());
    }
    
    @SuppressWarnings("rawtypes")
    private Flow makeFlow(int numDatums, int id) throws IOException {
        return makeFlow(numDatums, id, false);
    }
    
    @SuppressWarnings("rawtypes")
    private Flow makeFlow(int numDatums, int id, boolean fails) throws IOException {
        final Fields testFields = new Fields("user", "value");
        
        final String testDir = "build/test/FlowRunnerTest/testAsyncOperation/";
        String in = testDir + "in-" + id;
        String out = testDir + "out-" + id;

        Lfs sourceTap = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
        TupleEntryCollector write = sourceTap.openForWrite(new HadoopFlowProcess());
        
        for (int i = 0; i < numDatums; i++) {
            String username = "user-" + (i % 3);
            write.add(new Tuple(username, i));
        }
        
        write.close();

        Pipe pipe = new Pipe("test");
        pipe = new Each(pipe, new MyFilter(fails));
        Lfs sinkTap = new Lfs(new SequenceFile(testFields), out, SinkMode.REPLACE);

        Flow flow = new HadoopFlowConnector().connect("FlowRunnerTest", sourceTap, sinkTap, pipe);
        return flow;
    }

}
