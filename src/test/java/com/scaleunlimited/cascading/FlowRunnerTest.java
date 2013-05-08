package com.scaleunlimited.cascading;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.SumBy;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.util.Util;

import com.scaleunlimited.cascading.hadoop.HadoopPlatform;
import com.scaleunlimited.cascading.local.LocalPlatform;

public class FlowRunnerTest extends Assert {

    private enum MyCounters {
        FILTER_REQUESTS,
    }
    
    @SuppressWarnings({ "serial", "rawtypes" })
    private static class MyFilter extends BaseOperation implements Filter {

        private boolean _fails;
        private boolean _didDelay;
        
        public MyFilter(boolean fails) {
            _fails = fails;
            _didDelay = false;
        }
        
        @Override
        public boolean isRemove(FlowProcess process, FilterCall filterCall) {
            if (_fails) {
                throw new RuntimeException("We failed!");
            }
            
            if (!_didDelay) {
                _didDelay = true;
                
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    // ignore exception
                }
            }

            process.increment(MyCounters.FILTER_REQUESTS, 1);
            return false;
        }
    }
    
    @Test
    public void testAsyncOperation() throws Throwable {
        FlowRunner fr = new FlowRunner();
        Assert.assertTrue(fr.isDone());
        
        FlowFuture result0 = fr.addFlow(makeFlow("testAsyncOperation", 10, 0));
        FlowFuture result1 = fr.addFlow(makeFlow("testAsyncOperation", 100, 1));
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
        FlowFuture result = fr.addFlow(makeFlow("testShortWait", 100, 0));
        
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
        FlowFuture result = fr.addFlow(makeFlow("testFailureHandling", 100, 0, true));

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
        FlowFuture result = fr.addFlow(makeFlow("testCancelling", 100, 0, true));
        
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
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");
        
        // TODO It would be better to test with a larger capacity, but it only
        // runs one flow at a time in local mode.
        
        // An empty runner shouldn't be full.
        FlowRunner fr = new FlowRunner(1);
        Assert.assertFalse(fr.isFull());
        
        // There should be no room after we fill it up.
        FlowFuture result0 = fr.addFlow(makeFlow("testIsFull", 10, 0));
        Assert.assertTrue(fr.isFull());
        
        // There should be room after the first flow finishes.
        result0.get();
        Assert.assertFalse(fr.isFull());

        // There should be no room after we fill the empty slot.
        fr.addFlow(makeFlow("testIsFull", 10, 1));
        Assert.assertTrue(fr.isFull());

        // There should be room after everything completes.
        fr.complete();
        Assert.assertFalse(fr.isFull());
    }
    
    @Test
    public void testStatsLocal() throws Exception {
        final String logDirName = "build/test/testStatsLocal/log";
        BasePlatform platform = new LocalPlatform(FlowRunnerTest.class);
        FlowRunner fr = new FlowRunner("testStatsLocal", 1, new File(logDirName), 10);
        FlowFuture result0 = fr.addFlow(makeFlow("testStatsLocal", 10, 0, false, platform));
        result0.get();
        
        // We should some number of entries in the stats file
        checkStatsFile(logDirName, "testStatsLocal", "group on total", 1, 1);
    }
    
    @Test
    public void testStatsHadoop() throws Exception {
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

        final String logDirName = "build/test/testStatsHadoop/log";
        BasePlatform platform = new HadoopPlatform(FlowRunnerTest.class);
        FlowRunner fr = new FlowRunner("testStatsHadoop", 1, new File(logDirName), 1000L);
        FlowFuture result = fr.addFlow(makeFlow("testStatsHadoop", 10, 0, false, platform));
        result.get();
        
        // We should some number of entries in the stats file
        // Unfortunately you get no stats for Hadoop when running in Hadoop local mode, as there
        // is no JobTracker
        // checkStatsFile(logDirName, "testStatsHadoop", "group on total", 0, 1);
    }
    
    @Test
    public void testTerminationHadoop() throws Exception {
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

        BasePlatform platform = new HadoopPlatform(FlowRunnerTest.class);
        FlowRunner fr = new FlowRunner("testTerminationHadoop", 1, new File("build/test/testTerminationHadoop/log"), 100);
        fr.addFlow(makeFlow("testTerminationHadoop", 10, 0, false, platform));
        fr.terminate();
    }
    
    @Test
    public void testTerminationLocal() throws Exception {
        BasePlatform platform = new LocalPlatform(FlowRunnerTest.class);
        FlowRunner fr = new FlowRunner("testTerminationLocal", 1, new File("build/test/testTerminationLocal/log"), 10);
        fr.addFlow(makeFlow("testTerminationLocal", 10, 0, false, platform));
        fr.terminate();
    }
    
    @Test
    public void testStatsHadoopMiniCluster() throws Exception {
        // TODO create MiniClusterPlatform that extends HadoopPlatform, with
        // parameters to control log dir, temp dir, # map tasks, # reduce tasks,
        // and defaults for everything
        final String logDirName = "build/test/testStatsHadoopMiniCluster/log";
        System.setProperty("hadoop.log.dir", logDirName);

        if( Util.isEmpty(System.getProperty("hadoop.tmp.dir") ) )
            System.setProperty("hadoop.tmp.dir", "build/test/testStatsHadoopMiniCluster/tmp");

        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

        new File( System.getProperty("hadoop.log.dir")).mkdirs();

        JobConf conf = new JobConf();

        conf.setInt("mapred.job.reuse.jvm.num.tasks", -1 );

        MiniDFSCluster dfs = new MiniDFSCluster( conf, 4, true, null );
        FileSystem fileSys = dfs.getFileSystem();
        MiniMRCluster mr = new MiniMRCluster( 4, fileSys.getUri().toString(), 1, null, null, conf );

        JobConf jobConf = mr.createJobConf();

        jobConf.set("mapred.child.java.opts", "-Xmx128m");
        jobConf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
        jobConf.setInt("jobclient.completion.poll.interval", 50);
        jobConf.setInt("jobclient.progress.monitor.poll.interval", 50);
        jobConf.setMapSpeculativeExecution( false );
        jobConf.setReduceSpeculativeExecution( false );

        jobConf.setNumMapTasks(2);
        jobConf.setNumReduceTasks(2);

        BasePlatform platform = new HadoopPlatform(FlowRunnerTest.class, jobConf);
        platform.setJobPollingInterval(10);
        
        FlowRunner fr = new FlowRunner("testStatsHadoopMiniCluster", 1, platform.getDefaultLogDir(), 1000);
        FlowFuture result0 = fr.addFlow(makeFlow("testStatsHadoopMiniCluster", 10, 0, false, platform));
        result0.get();
        
        // We should some number of entries in the stats file
        checkStatsFile(logDirName, "testStatsHadoopMiniCluster", "group on total", 0, 2);
    }
    
    private BufferedReader openStatsFile(String logDirName, String testName) throws FileNotFoundException {
        File statsDir = new File(logDirName);
        File statsFile = new File(statsDir, testName + "-stats.tsv");
        assertTrue(statsFile.exists());
        
        return new BufferedReader(new FileReader(statsFile));
    }
    
    private void checkStatsFile(String logDirName, String testName, String stepName, int numMaps, int numReduces) throws IOException {
        String targetText = String.format("\t%d\t%d\t%s|%s=%d,%d;", numMaps, numReduces, testName, stepName, numMaps, numReduces);
        BufferedReader br = openStatsFile(logDirName, testName);
        
        String curLine;
        while ((curLine = br.readLine()) != null) {
            if (curLine.contains(targetText)) {
                return;
            }
        }
        
        fail("Couldn't find target line in stats file");
    }
    
    @SuppressWarnings("rawtypes")
    private Flow makeFlow(String testName, int numDatums, int id) throws Exception {
        return makeFlow(testName, numDatums, id, false);
    }
    
    @SuppressWarnings("rawtypes")
    private Flow makeFlow(String testName, int numDatums, int id, boolean fails) throws Exception {
        return makeFlow(testName, numDatums, id, fails, new HadoopPlatform(FlowRunnerTest.class));
    }
    
    @SuppressWarnings("rawtypes")
    private Flow makeFlow(String testName, int numDatums, int id, boolean fails, BasePlatform platform) throws Exception {
        final Fields testFields = new Fields("user", "value");
        
        BasePath testDir = platform.makePath("build/test/FlowRunnerTest/" + testName + "/");
        BasePath in = platform.makePath(testDir, "in-" + id);
        Tap sourceTap = platform.makeTap(platform.makeBinaryScheme(testFields), in, SinkMode.REPLACE);
        TupleEntryCollector write = sourceTap.openForWrite(platform.makeFlowProcess());
        
        for (int i = 0; i < numDatums; i++) {
            String username = "user-" + (i % 3);
            write.add(new Tuple(username, i));
        }
        
        write.close();

        Pipe pipe = new Pipe("test");
        pipe = new Each(pipe, new MyFilter(fails));
        pipe = new SumBy("sum values", pipe, new Fields("user"), new Fields("value"), new Fields("total"), Integer.class);
        pipe = new GroupBy("group on total", pipe, new Fields("total"));
        
        BasePath out = platform.makePath(testDir, "out-" + id);
        Tap sinkTap = platform.makeTap(platform.makeBinaryScheme(new Fields("user", "total")), out, SinkMode.REPLACE);

        Flow flow = platform.makeFlowConnector().connect(testName, sourceTap, sinkTap, pipe);
        FlowUtils.nameFlowSteps(flow);
        return flow;
    }

}
