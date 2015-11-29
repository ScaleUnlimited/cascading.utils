package com.scaleunlimited.cascading;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.FlowStep;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.stats.CascadingStats.Status;
import cascading.stats.FlowStats;
import cascading.stats.FlowStepStats;
import cascading.stats.hadoop.HadoopSliceStats;
import cascading.stats.hadoop.HadoopSliceStats.Kind;
import cascading.stats.hadoop.HadoopStepStats;
import cascading.stats.local.LocalStepStats;

import com.scaleunlimited.cascading.hadoop.HadoopUtils;

public class FlowRunner {
    static final Logger LOGGER = LoggerFactory.getLogger(FlowRunner.class);

    private static final long FLOW_CHECK_INTERVAL = 1 * 1000L;
    private static final long TERMINATE_CHECK_INTERVAL = 100L;

    // Default number of flows to run in parallel
    private static final int DEFAULT_MAX_FLOWS = 100;

    // Maximum number of map or reduce slots (flow name + step name) in memory.
    protected static final int MAX_TIME_SLOTS = 10000;
    
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
        this(DEFAULT_MAX_FLOWS);
    }
    
    public FlowRunner(int maxFlows) {
        this("FlowRunner", maxFlows, null, 0);
    }
    
    /**
     * Create a FlowRunner suitable for running up to maxFlows flows in parallel.
     * Note that maxFlows is ignored (treated as 1) when adding a flow, if we're
     * not in a real (non-local) Hadoop environment.
     * 
     * @param runnerName Name of the FlowRunner, for stats & job naming purposes.
     * @param maxFlows Maximum number of flows to run in parallel
     * @param statsDir Where to put continuous job statistics (or null for no stats)
     * @param checkInterval How often to write out statistics, in milliseconds.
     */
    public FlowRunner(String runnerName, int maxFlows, File statsDir, final long checkInterval) {
        if ((maxFlows > 1) && (HadoopUtils.isJobLocal(new JobConf()))) {
            LOGGER.warn("Running locally, so flows must be run serially for thread safety.");
            _maxFlows = 1;
        } else {
            _maxFlows = maxFlows;
        }
        
        createStats(runnerName, statsDir, checkInterval);
    }

    
    private void createStats(final String runnerName, File statsDir, final long checkInterval) {
        if (statsDir == null) {
            return;
        }

        // Since the caller wants stats, we need to fire up a thread that will check
        // the flows on a regular basis.
        statsDir.mkdirs();
        
        final PrintStream statsStream = makeStatsStream(statsDir, runnerName, "stats.tsv");
        final PrintStream detailsStream = makeStatsStream(statsDir, runnerName, "details.tsv");
        final PrintStream summaryStream = makeStatsStream(statsDir, runnerName, "summary.tsv");
        
        _statsThread = new Thread(new Runnable() {

            @Override
            public void run() {
                long startTime = System.currentTimeMillis();

                // We want to set our next check time so that when the checkInterval is
                // added to it, we're on a checkInterval boundary. So that way we'd
                // get a check time of say 03:34:57, and the next check time would
                // be 03:35:00, if the interval was every minute.
                long nextCheckTime = startTime - (startTime % checkInterval);

                SimpleDateFormat timeFormatter = new SimpleDateFormat("yyyyMMdd hh:mm:ss");
                timeFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
                
                // Set up for per-step summaries. For each unique step (Hadoop Job), we'll
                // keep track of total map-minutes, total reduce-minutes.
                Map<String, Long> mapMilliseconds = new HashMap<String, Long>();
                Map<String, Long> reduceMilliseconds = new HashMap<String, Long>();
                
                try {
                    boolean terminating = false;
                    
                    while (!terminating) {
                        // We sleep at the top, so that when we get interrupted (typically when a flow is done)
                        // we collect the final stats for the flow.
                        try {
                            Thread.sleep(Math.max(0, nextCheckTime - System.currentTimeMillis()));
                        } catch (InterruptedException e) {
                            // We were interrupted, so terminate.
                            LOGGER.info("Terminating Flow stats thread");
                            terminating = true;
                        }

                        Map<String, TaskStats> taskCounts = new HashMap<String, TaskStats>();

                        String timestamp = timeFormatter.format(nextCheckTime);
                        nextCheckTime += checkInterval;
                        synchronized (_flowFutures) {
                            for (FlowFuture ff : _flowFutures) {
                                collectStats(ff, taskCounts);
                            }
                        }
                        
                        // Output counts. Format is
                        // <timestamp><tab><map tasks><tab><reduce tasks><tab><task details>
                        String stats = makeStats(taskCounts, true);
                        statsStream.println(String.format("%s\t%s", timestamp, stats));
                        // System.out.println("" + timestamp + "\t" + stats);

                        // Generate a summary line
                        // <timestamp>    <map tasks>    <reduce tasks>    
                        stats = makeStats(taskCounts, false);
                        detailsStream.println(String.format("%s\t%s", timestamp, stats));

                        // For each of the tasks with map/reduce counts, output a separate line after the summary line.
                        //     <map tasks>    <reduce tasks>    <task name>    <timestamp>
                        for (TaskStats taskStat : taskCounts.values()) {
                            String flowAndStepName = String.format("%s|%s", taskStat.getFlowName(), taskStat.getStepName());
                            
                            int mapCount = taskStat.getMapCount();
                            addSlotTime(mapMilliseconds, flowAndStepName, mapCount, checkInterval);
                            
                            int reduceCount = taskStat.getReduceCount();
                            addSlotTime(reduceMilliseconds, flowAndStepName, reduceCount, checkInterval);
                            
                            if (mapCount + reduceCount > 0) {
                                detailsStream.println(String.format("\t%d\t%d\t%s\t%s",
                                                mapCount, reduceCount,
                                                flowAndStepName,
                                                timestamp));
                            }
                        }
                    }
                    
                    // All done with Flow, write out summary results.
                    for (String flowAndStepName : mapMilliseconds.keySet()) {
                        long mapDuration = mapMilliseconds.get(flowAndStepName) / 60 * 1000L;
                        long reduceDuration = reduceMilliseconds.get(flowAndStepName) / 60 * 1000L;
                        summaryStream.println(String.format("%d\t%d\t%s", mapDuration, reduceDuration, flowAndStepName));
                    }
                    
                } catch (Throwable t) {
                    LOGGER.error("Exception while collecting stats for " + runnerName, t);
                } finally {
                    IOUtils.closeQuietly(statsStream);
                    IOUtils.closeQuietly(detailsStream);
                    IOUtils.closeQuietly(summaryStream);
                }
            }

            private void addSlotTime(Map<String, Long> slotTime, String flowAndStepName, int slotCount, long checkInterval) {
                Long curSlotTime = slotTime.get(flowAndStepName);
                if (curSlotTime == null) {
                    int numSlots = slotTime.size();

                    if (numSlots > MAX_TIME_SLOTS) {
                        return;
                    } else if (numSlots == MAX_TIME_SLOTS) {
                        // Generate a single error.
                        LOGGER.error("At max number of time slots, won't record additional flow/step combinations");
                    }
                    
                    curSlotTime = new Long(0);
                }
                
                slotTime.put(flowAndStepName, curSlotTime + (slotCount * checkInterval));
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

    private PrintStream makeStatsStream(File statsDir, String runnerName, String suffix) {
        File statsFile = new File(statsDir, runnerName + "-" + suffix);
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

        return statsStream;
    }

    private String makeStats(Map<String, TaskStats> taskCounts, boolean includeTaskDetails) {
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
                
                if (includeTaskDetails) {
                    taskDetails.append(String.format("%s|%s=%d,%d; ", stats.getFlowName(), stats.getStepName(), stats.getMapCount(), stats.getReduceCount()));
                }
            }
        }
        
        return String.format("%d\t%d\t%s", mapCount, reduceCount, taskDetails.toString());
    }


    private void collectStats(FlowFuture ff, Map<String, TaskStats> taskCounts) {
        Flow flow = ff.getFlow();
        FlowStats fs = flow.getFlowStats();

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
               // We don't want/need info on task attempts
               hadoopSS.captureDetail(false);
               
               // We have one child for every task. We have to see if it's
               // running, and if so, whether it's a mapper or reducer
               Iterator<HadoopSliceStats> iter = hadoopSS.getChildren().iterator();
               while (iter.hasNext()) {
                   HadoopSliceStats sliceStats = iter.next();
                   // System.out.println(String.format("id=%s, kind=%s, status=%s", sliceStats.getID(), sliceStats.getKind(), sliceStats.getStatus()));
                   
                   if (sliceStats.getStatus() == Status.SUCCESSFUL) {
                       // Set the total time
                       // TODO this doesn't seem to be working, I get 0.
                       // Plus it needs JobInProgress.Counter as a class, which means anyone using
                       // cascading.utils winds up needing to include Hadoop.
                       /*
                       incrementCounts(taskCounts, countsKey, flowName, stepName, 
                                       0,
                                       0, 
                                       sliceStats.getCounterValue(JobInProgress.Counter.SLOTS_MILLIS_MAPS), 
                                       sliceStats.getCounterValue(JobInProgress.Counter.SLOTS_MILLIS_REDUCES));
                       */
                   } else if (sliceStats.getStatus() == Status.RUNNING) {
                       if (sliceStats.getKind() == Kind.MAPPER) {
                           incrementCounts(taskCounts, countsKey, flowName, stepName, 1, 0, 0, 0);
                       } else if (sliceStats.getKind() == Kind.REDUCER) {
                           incrementCounts(taskCounts, countsKey, flowName, stepName, 0, 1, 0, 0);
                       }
                   }
               }
           } else if (stepStats instanceof LocalStepStats) {
               stepStats.captureDetail();
               
               // map & reduce kind of run as one, so just add one to both if there's a group.
               incrementCounts(taskCounts, countsKey, flowName, stepName, 1, 0, 0, 0);
               if (flowStep.getGroups().size() > 0) {
                   incrementCounts(taskCounts, countsKey, flowName, stepName, 0, 1, 0, 0);
               }
           } else {
               throw new RuntimeException("Unknown type returned by FlowStep.getFlowStepStats: " + stepStats.getClass());
           }
        }
    }

    /**
     * Cascading will hang on to the HadoopStepStats for every flow, until we get rid of them. So we'll clear out
     * stats once a FlowFuture is done.
     */
    private void clearStats(FlowFuture ff) {
        Flow flow = ff.getFlow();
        FlowStats fs = ff.getFlow().getFlowStats();

        List<FlowStep> flowSteps = flow.getFlowSteps();
        for (FlowStep flowStep : flowSteps) {
           FlowStepStats stepStats = flowStep.getFlowStepStats();
           
           if (stepStats instanceof HadoopStepStats) {
               HadoopStepStats hadoopSS = (HadoopStepStats)stepStats;
               hadoopSS.getTaskStats().clear();
           }
        }
    }
    
    private void incrementCounts(Map<String, TaskStats> counts, String countsKey, String flowName, String stepName, 
                    int mapCount, int reduceCount, long mapTime, long reduceTime) {
        // If they're all zero, just ignore the call so we don't get extra entries.
        if ((mapCount == 0) && (reduceCount == 0) && (mapTime == 0) && (reduceTime == 0)) {
            return;
        }
        
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
        
        // Assume single-threaded unless we're a non-local Hadoop flow.
        boolean singleThreaded = true;
        FlowProcess<?> fp = HadoopUtils.undelegate(flow.getFlowProcess());
        if (fp instanceof HadoopFlowProcess) {
            HadoopFlowProcess hfp = (HadoopFlowProcess)fp;
            singleThreaded = HadoopUtils.isJobLocal(hfp.getJobConf());
        }
        String message;
        if (singleThreaded) {
            message = 
                String.format(  "Adding flow %s to single-threaded FlowRunner",
                                flow.getName());
        } else {
            message = 
                String.format(  "Adding flow %s to FlowRunner (which supports %d simultaneous flows)",
                                flow.getName(),
                                _maxFlows);
        }
        LOGGER.info(message);
        
        // Find an open spot, or loop until we get one.
        while (true) {
            synchronized (_flowFutures) {
                Iterator<FlowFuture> iter = _flowFutures.iterator();
                while (iter.hasNext()) {
                    FlowFuture ff = iter.next();
                    if (ff.isDone()) {
                        clearStats(ff);
                        iter.remove();
                    }
                }

                // Now that we've removed any flows that are done, see if we
                // can add the new flow. If we're single threaded then the
                // max number of flows at one time is just one.
                if (_flowFutures.size() < (singleThreaded ? 1 : _maxFlows)) {
                    FlowFuture ff = new FlowFuture(flow);
                    _flowFutures.add(ff);
                    return ff;
                }
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
        
        synchronized (_flowFutures) {
            return (_flowFutures.size() >= _maxFlows);
        }
    }
    
    /**
     * Return true if all of the flows are done running.
     * 
     * @return
     */
    public boolean isDone() {
        synchronized (_flowFutures) {
            Iterator<FlowFuture> iter = _flowFutures.iterator();
            while (iter.hasNext()) {
                FlowFuture ff = iter.next();
                if (ff.isDone()) {
                    iter.remove();
                } else {
                    return false;
                }
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
        // First terminate all of the running flows.
        synchronized (_flowFutures) {
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
        
        // And now interrupt the stats thread, as we should now have collected
        // all of the data.
        Thread statsThread = null;
        if (_statsThread != null) {
            synchronized(_statsThread) {
                // Somebody might have cleared the thread
                if ((_statsThread != null) && _statsThread.isAlive()) {
                    _statsThread.interrupt();
                    // Save thread off, since we have to leave the synchronized
                    // block before the actual running code can clear the thread
                    // state.
                    statsThread = _statsThread;
                    _statsThread = null;
                }
            }
        }
        
        // Now we need to wait until the stats thread has really stopped,
        // so that stats results are available after a call to terminate().
        // This is mostly so that unit tests can reliably run.
        if (statsThread != null) {
            // Wait until the stats thread has really stopped
            while (statsThread.isAlive()) {
                try {
                    Thread.sleep(TERMINATE_CHECK_INTERVAL);
                } catch (InterruptedException e) {
                    LOGGER.warn("Stats thread termination interrupted!");
                }
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
