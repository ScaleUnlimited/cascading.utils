package com.scaleunlimited.cascading;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import cascading.flow.Flow;
import cascading.flow.FlowListener;

public class FlowFuture implements Future<FlowResult> {

    private static final long FLOW_SLEEP_TIME = 1 * 1000L;
    
    private Flow _flow;
    
    private volatile Throwable _flowException;
    private volatile boolean _canceled;
    private volatile boolean _done;
    
    public FlowFuture(Flow flow) {
        _flow = flow;
        
        _flowException = null;
        FlowListener catchExceptions = new FlowListener() {

            @Override
            public void onCompleted(Flow flow) {
                _done = true;
            }

            @Override
            public void onStarting(Flow flow) { }

            @Override
            public void onStopping(Flow flow) {
                _canceled = true;
            }

            @Override
            public boolean onThrowable(Flow flow, Throwable t) {
                _flowException = t;
                return true;
            }
        };
        
        _flow.addListener(catchExceptions);
        _flow.start();
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (_canceled || _done) {
            return false;
        }
        
        // We start running right away, so we have to interrupt.
        if (!mayInterruptIfRunning) {
            return false;
        }
        
        // Stop the flow. This will (eventually) set up the _done & _cancelled flags.
        FlowUtils.safeStop(_flow);
        
        // Wait until the onStopping AND onComplete listeners have been called.
        while (!_canceled || !_done) {
            try {
                Thread.sleep(FLOW_SLEEP_TIME);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        return true;
    }

    public Flow getFlow() {
        return _flow;
    }
    
    @Override
    public FlowResult get() throws InterruptedException, ExecutionException {
        while (!_done) {
            Thread.sleep(FLOW_SLEEP_TIME);
        }
        
        if (_canceled) {
            throw new CancellationException();
        }
        
        if (_flowException != null) {
            throw new ExecutionException(_flowException);
        }
        
        return makeFlowResult();
    }

    @Override
    public FlowResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        long remainingTimeNS = unit.toNanos(timeout);
        final long defaultWaitNS = TimeUnit.MILLISECONDS.toNanos(FLOW_SLEEP_TIME);
        
        while (!_done && (remainingTimeNS > 0)) {
            long waitTimeNS = Math.min(defaultWaitNS, remainingTimeNS);
            remainingTimeNS -= waitTimeNS;
            
            long ms = TimeUnit.NANOSECONDS.toMillis(waitTimeNS);
            int ns = (int)(waitTimeNS - TimeUnit.MILLISECONDS.toNanos(ms));

            Thread.sleep(ms, ns);
        }
        
        if (_canceled) {
            throw new CancellationException();
        }

        if (!_done) {
            throw new TimeoutException();
        }
        
        if (_flowException != null) {
            throw new ExecutionException(_flowException);
        }

        return makeFlowResult();
    }

    @Override
    public boolean isCancelled() {
        return _canceled;
    }

    @Override
    public boolean isDone() {
        return _done;
    }
    
    private FlowResult makeFlowResult() {
//        Map<String, Long> result = new HashMap<String, Long>();
//
//        FlowStats stats = _flow.getFlowStats();
//        Collection<String> counterGroups = stats.getCounterGroups();
//        for (String counterGroup : counterGroups) {
//            Collection<String> counters = stats.getCountersFor(counterGroup);
//            for (String counter : counters) {
//                long counterValue = stats.getCounterValue(counterGroup, counter);
//                String counterName = counterGroup + "." + counter;
//                if (result.containsKey(counterName)) {
//                    result.put(counterName, counterValue + result.get(counterName));
//                } else {
//                    result.put(counterName, counterValue);
//                }
//            }
//        }
//
        return new FlowResult(FlowCounters.getCounters(_flow));
    }
    

}
