package com.scaleunlimited.cascading;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.log4j.Logger;

import com.scaleunlimited.cascading.hadoop.HadoopUtils;

import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.stats.CascadingStats.Status;
import cascading.stats.FlowStats;
import cascading.stats.FlowStepStats;
import cascading.stats.hadoop.HadoopSliceStats;
import cascading.stats.hadoop.HadoopSliceStats.Kind;
import cascading.stats.hadoop.HadoopStepStats;
import cascading.stats.local.LocalStepStats;

public class FlowRunner {
    static final Logger LOGGER = Logger.getLogger(FlowRunner.class);

    private static final long FLOW_CHECK_INTERVAL = 5 * 1000L;
    
    private static final long DEFAULT_STATS_CHECK_INTERVAL = 15 * 60 * 1000L;

    private static class TaskStats {
        
        private String _flowName;
        private String _stepName;
        
        private int _mapCount;
        private int _reduceCount;
        
        private long _mapTime;
        private long _reduceTime;
        
        public TaskStats(String flowName, String stepName) {
            _flowName = flowName;
            _stepName = stepName;
            
            _mapCount = 0;
            _reduceCount = 0;
            
            _mapTime = 0;
            _reduceTime = 0;
        }
        
        public void incMapCount(int mapCount) {
            _mapCount += mapCount;
        }
        
        public void incReduceCount(int reduceCount) {
            _reduceCount += reduceCount;
        }

        public void incMapTime(long mapTime) {
            _mapTime += mapTime;
        }
        
        public void incReduceTime(long reduceTime) {
            _reduceTime += reduceTime;
        }

        public String getFlowName() {
            return _flowName;
        }

        public String getStepName() {
            return _stepName;
        }

        public int getMapCount() {
            return _mapCount;
        }

        public int getReduceCount() {
            return _reduceCount;
        }
        
        public long getMapTime() {
            return _mapTime;
        }
        
        public long getReduceTime() {
            return _reduceTime;
        }
    }
    
    private int _maxFlows;
    private final List<FlowFuture> _flowFutures = new ArrayList<FlowFuture>();
    
    private Thread _statsThread;
    
    public FlowRunner() {
        this(Integer.MAX_VALUE);
    }
    
    public FlowRunner(int maxFlows) {
        this("FlowRunner", maxFlows, null, DEFAULT_STATS_CHECK_INTERVAL);
    }
    
    public FlowRunner(String runnerName, int maxFlows, File statsDir, final long checkInterval) {
        if ((maxFlows > 1) && (HadoopUtils.isJobLocal(new JobConf()))) {
            LOGGER.warn("Running locally, so flows must be run serially for thread safety.");
            _maxFlows = 1;
        } else {
            _maxFlows = maxFlows;
        }
        
        // If the caller wants stats, we need to fire up a thread that will check
        // the flows on a regular basis.
        if (statsDir != null) {
            statsDir.mkdirs();
            File statsFile = new File(statsDir, runnerName + "-stats.tsv");
            statsFile.delete();
            
            final PrintStream statsStream;

            try {
                statsStream = new PrintStream(statsFile, "UTF-8");
                LOGGER.info("Logging stats to " + statsFile);
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Can't create stats output file: " + statsFile, e);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("Impossible exception", e);
            }
            
            _statsThread = new Thread(new Runnable() {
                
                @Override
                public void run() {
                    long startTime = System.currentTimeMillis();
                    
                    // We want to set our next check time so that when the checkInterval is
                    // added to it, we're on a checkInterval boundary. So that way we'd
                    // get a check time of say 03:34:57, and the next check time would
                    // be 03:35:00, if the interval was every minute.
                    long nextCheckTime = startTime - (startTime % checkInterval);
                    
                    SimpleDateFormat timeFormatter = new SimpleDateFormat("yyyyddMM hh:mm:ss");
                    
                    while (true) {
                        Map<String, TaskStats> taskCounts = new HashMap<String, TaskStats>();

                        String timestamp = timeFormatter.format(nextCheckTime);
                        nextCheckTime += checkInterval;
                        for (FlowFuture ff : _flowFutures) {
                            collectStats(ff, taskCounts);
                        }
                        
                        // Output counts. Format is
                        // <timestamp><tab><map tasks><tab><reduce tasks><tab><task details>
                        String stats = makeStats(taskCounts);
                        statsStream.println(String.format("%s\t%s", timestamp, stats));
                        // System.out.println("" + timeInMinutes + "\t" + stats);
                        
                        try {
                            Thread.sleep(Math.max(0, nextCheckTime - System.currentTimeMillis()));
                        } catch (InterruptedException e) {
                            // We were interrupted, so quietly terminate.
                            break;
                        }
                    }
                    
                    IOUtils.closeQuietly(statsStream);
                }

            }, "FlowRunner stats");
            
            // We don't want to hold up the JVM for this one thread.
            _statsThread.setDaemon(true);
            
            // We want to make sure the thread gets terminated on shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

                @Override
                public void run() {
                    terminate();
                }
            }, "FlowRunner stats shutdown hook"));
            
            _statsThread.start();
        }
    }

    private String makeStats(Map<String, TaskStats> taskCounts) {
        // We want <# map tasks><tab><# reduce tasks><tab><task details>
        // where <task details> looks like <flowname|stepname=mapcount,reducecount; flowname|stepname=mapcount,reducecount>
        int mapCount = 0;
        int reduceCount = 0;
        StringBuilder taskDetails = new StringBuilder();
        
        for (TaskStats stats : taskCounts.values()) {
            
            if ((stats.getMapCount() == 0) && (stats.getReduceCount() == 0)) {
                // TODO Figure out why we don't get any total map/reduce time values.
                // taskDetails.append(String.format("%s|%s=%dms,%dms; ", stats.getFlowName(), stats.getStepName(), stats.getMapTime(), stats.getReduceTime()));
            } else {
                mapCount += stats.getMapCount();
                reduceCount += stats.getReduceCount();
                
                taskDetails.append(String.format("%s|%s=%d,%d; ", stats.getFlowName(), stats.getStepName(), stats.getMapCount(), stats.getReduceCount()));
            }
        }
        
        return String.format("%d\t%d\t%s", mapCount, reduceCount, taskDetails.toString());
    }


    private void collectStats(FlowFuture ff, Map<String, TaskStats> taskCounts) {
        Flow flow = ff.getFlow();
        FlowStats fs = flow.getFlowStats();
        fs.captureDetail();

        String flowId = flow.getID();
        String flowName = flow.getName();
        
        List<FlowStep> flowSteps = flow.getFlowSteps();
        for (FlowStep flowStep : flowSteps) {
           FlowStepStats stepStats = flowStep.getFlowStepStats();

           String stepId = flowStep.getID();
           String stepName = flowStep.getName();
           
           String countsKey = String.format("%s-%s", flowId, stepId);
           if (stepStats instanceof HadoopStepStats) {
               HadoopStepStats hadoopSS = (HadoopStepStats)stepStats;
               
               // We have one child for every task. We have to see if it's
               // running, and if so, whether it's a mapper or reducer
               Iterator<HadoopSliceStats> iter = hadoopSS.getChildren().iterator();
               while (iter.hasNext()) {
                   HadoopSliceStats sliceStats = iter.next();
                   // System.out.println(String.format("id=%s, kind=%s, status=%s", sliceStats.getID(), sliceStats.getKind(), sliceStats.getStatus()));
                   
                   if (sliceStats.getStatus() == Status.SUCCESSFUL) {
                       // Set the total time
                       incrementCounts(taskCounts, countsKey, flowName, stepName, 
                                       0,
                                       0, 
                                       sliceStats.getCounterValue(JobInProgress.Counter.SLOTS_MILLIS_MAPS), 
                                       sliceStats.getCounterValue(JobInProgress.Counter.SLOTS_MILLIS_REDUCES));
                   } else if (sliceStats.getStatus() == Status.RUNNING) {
                       if (sliceStats.getKind() == Kind.MAPPER) {
                           incrementCounts(taskCounts, countsKey, flowName, stepName, 1, 0, 0, 0);
                       } else if (sliceStats.getKind() == Kind.REDUCER) {
                           incrementCounts(taskCounts, countsKey, flowName, stepName, 0, 1, 0, 0);
                       }
                   }
               }
           } else if (stepStats instanceof LocalStepStats) {
               // map & reduce kind of run as one, so just add one to both if there's a group.
               incrementCounts(taskCounts, countsKey, flowName, stepName, 1, 0, 0, 0);
               if (flowStep.getGroup() != null) {
                   incrementCounts(taskCounts, countsKey, flowName, stepName, 0, 1, 0, 0);
               }
           } else {
               throw new RuntimeException("Unknown type returned by FlowStep.getFlowStepStats: " + stepStats.getClass());
           }
        }
    }

    private void incrementCounts(Map<String, TaskStats> counts, String countsKey, String flowName, String stepName, int mapCount, int reduceCount, long mapTime, long reduceTime) {
        TaskStats curStats = counts.get(countsKey);
        if (curStats == null) {
            curStats = new TaskStats(flowName, stepName);
        }
        
        curStats.incMapCount(mapCount);
        curStats.incReduceCount(reduceCount);
        curStats.incMapTime(mapTime);
        curStats.incReduceTime(reduceTime);
        
        counts.put(countsKey, curStats);
    }

    /**
     * Wait for an open slot, and then start the Flow running, returning
     * the corresponding FlowFuture.
     * 
     * FUTURE - we could make FlowFuture something where the flow doesn't
     * start until you call FlowFuture.start(). Then this routine could
     * immediately return the FlowFuture, and start it when we drop under
     * <maxFlows>. But that would require an async executor to constantly
     * be polling the running flows, determining when they are done.
     * 
     * @param flow the Flow to run
     * @return FlowFuture
     * @throws InterruptedException
     */
    public FlowFuture addFlow(Flow flow) throws InterruptedException {
        
        // Find an open spot, or loop until we get one.
        while (true) {
            Iterator<FlowFuture> iter = _flowFutures.iterator();
            while (iter.hasNext()) {
               FlowFuture ff = iter.next();
                if (ff.isDone()) {
                    iter.remove();
                }
            }
            
            // Now that we've removed any flows that are done, see if we
            // can add the new flow.
            if (_flowFutures.size() < _maxFlows) {
                FlowFuture ff = new FlowFuture(flow);
                _flowFutures.add(ff);
                return ff;
            }
            
            // No open slots, so loop
            Thread.sleep(FLOW_CHECK_INTERVAL);
        }
    }
    
    /**
     * @return false if there is room to add at least one more flow.
     */
    public boolean isFull() {
        
        // Release any completed flows
        isDone();
        
        return (_flowFutures.size() >= _maxFlows);
    }
    
    /**
     * Return true if all of the flows are done running.
     * 
     * @return
     */
    public boolean isDone() {
        Iterator<FlowFuture> iter = _flowFutures.iterator();
        while (iter.hasNext()) {
            FlowFuture ff = iter.next();
            if (ff.isDone()) {
                iter.remove();
            } else {
                return false;
            }
        }
        
        // Nothing still running, so we're all done.
        return true;
    }
    
    /**
     * Wait until all of the flows have finished.
     * 
     * @throws InterruptedException
     */
    public void complete() throws InterruptedException {
        while (!isDone()) {
            Thread.sleep(FLOW_CHECK_INTERVAL);
        }
        
        terminate();
    }
    
    public void terminate() {
        if (_statsThread != null) {
            synchronized(_statsThread) {
                // Somebody might have cleared the thread
                if ((_statsThread != null) && _statsThread.isAlive()) {
                    _statsThread.interrupt();
                    _statsThread = null;
                }
            }
        }
        
        // Now terminate all of the running flows.
        Iterator<FlowFuture> iter = _flowFutures.iterator();
        while (iter.hasNext()) {
            FlowFuture ff = iter.next();
            if (ff.isDone()) {
                iter.remove();
            } else {
                ff.cancel(true);
            }
        }
    }
    
    @Override
    protected void finalize() throws Throwable {
        terminate();
        
        super.finalize();
    }
    
    /**
     * Convenience method for running a Flow and returning the result.
     * 
     * @param flow Flow to run
     * @return Result of running the flow.
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static FlowResult run(Flow flow) throws InterruptedException, ExecutionException {
        FlowFuture ff = new FlowFuture(flow);
        return ff.get();
    }
}
