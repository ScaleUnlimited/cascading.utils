package com.bixolabs.cascading;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import cascading.flow.Flow;

public class FlowRunner {
    static final Logger LOGGER = Logger.getLogger(FlowRunner.class);

    private static final long FLOW_CHECK_INTERVAL = 5 * 1000L;
    
    private int _maxFlows;
    private List<FlowFuture> _flowFutures;
    
    public FlowRunner() {
        this(Integer.MAX_VALUE);
    }
    
    public FlowRunner(int maxFlows) {
        if ((maxFlows > 1) && (HadoopUtils.isJobLocal(new JobConf()))) {
            LOGGER.warn("Running locally, so flows must be run serially for thread safety.");
            _maxFlows = 1;
        } else {
            _maxFlows = maxFlows;
        }
        _flowFutures = new ArrayList<FlowFuture>();
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
     * Return true if all of the flows are done running.
     * 
     * @return
     */
    public boolean isDone() {
        for (FlowFuture ff : _flowFutures) {
            if ((!ff.isDone())) {
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
