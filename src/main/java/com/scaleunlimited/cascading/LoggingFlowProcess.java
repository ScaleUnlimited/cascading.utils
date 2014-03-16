/**
 * Copyright 2010-2013 Scale Unlimited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.scaleunlimited.cascading;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.cascading.hadoop.HadoopUtils;

import cascading.flow.FlowProcess;
import cascading.flow.FlowProcessWrapper;
import cascading.flow.hadoop.HadoopFlowProcess;

public class LoggingFlowProcess<Config> extends FlowProcessWrapper<Config> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingFlowProcess.class);

    // enum used for counting number of occurrences of each type of msg.
    public static enum LoggingLevels {
        TRACE,
        DEBUG,
        INFO,
        WARN,
        ERROR;
        
        public static LoggingLevels fromLevel(Level level) {
            switch (level) {
                case SLF4J_TRACE: return TRACE;
                case SLF4J_DEBUG: return DEBUG;
                case SLF4J_ERROR: return ERROR;
                case SLF4J_INFO: return INFO;
                case SLF4J_WARN: return WARN;
                default: throw new RuntimeException("Unknown level: " + level);
            }
        }
    }
    
    private class HadoopFlowReporter implements IFlowReporter {
        private Reporter _reporter;

        public HadoopFlowReporter(Reporter reporter) {
            _reporter = reporter;
        }

        // TODO VMa: fix for slf4j
        @Override
        public void setStatus(Level level, String msg) {
            if ((_reporter != null) && level.isGreaterOrEqual(Level.SLF4J_INFO)) {
                _reporter.setStatus("Cascading " + level + ": " + msg);
            }
        }

        @Override
        public void setStatus(String msg, Throwable t) {
            // TODO KKr - add stringified <t> (maybe from Nutch?) to msg.
            if (_reporter != null) {
                _reporter.setStatus("Cascading " + Level.SLF4J_ERROR + ": " + msg);
            }
        }
    }

    private boolean _isLocal;
    private List<IFlowReporter> _reporters;
    private Map<Enum, AtomicLong> _localCounters;

    public LoggingFlowProcess(FlowProcess<Config> baseProcess, IFlowReporter reporter) {
        super(baseProcess);
        init(HadoopUtils.undelegate(baseProcess), reporter);
    }

    public LoggingFlowProcess(FlowProcess<Config> baseProcess) {
        super(baseProcess);
        
        FlowProcess dfp = HadoopUtils.undelegate(baseProcess);
        if (dfp instanceof HadoopFlowProcess) {
            init(dfp, new HadoopFlowReporter(((HadoopFlowProcess)dfp).getReporter()));
        } else {
            init(dfp, new LoggingFlowReporter());
        }
    }
    
    public LoggingFlowProcess(HadoopFlowProcess baseProcess) {
        super(baseProcess);
        
        IFlowReporter reporter = new HadoopFlowReporter(baseProcess.getReporter());
        init(HadoopUtils.undelegate(baseProcess), reporter);
    }

    /**
     * A no-argument constructor for use during testing, when we don't have a
     * real Cascading FlowProcess to use.
     */
    public LoggingFlowProcess() {
        super(FlowProcess.NULL);
        
        init(HadoopUtils.undelegate(FlowProcess.NULL), new LoggingFlowReporter());
    }

    /**
     * @param delegateProcess that might have been hidden inside a
     * FlowProcessWrapper, where the latter was passed as baseProcess to the
     * constructor (i.e., you should pass the result of
     * {@link HadoopUtils#undelegate(FlowProcess)}) to this method.
     * @param reporter where logging will be directed.
     */
    private void init(FlowProcess delegateProcess, IFlowReporter reporter) {
        _isLocal =
            (   (!(delegateProcess instanceof HadoopFlowProcess))
            ||  HadoopUtils.isJobLocal(((HadoopFlowProcess) delegateProcess).getJobConf()));

        _localCounters = new HashMap<Enum, AtomicLong>();
        _reporters = new ArrayList<IFlowReporter>();
        addReporter(reporter);
    }

    public void addReporter(IFlowReporter reporter) {
        _reporters.add(reporter);
    }
    
//    @SuppressWarnings("deprecation")
//    public JobConf getJobConf() throws IOException {
//        if (getDelegate() instanceof HadoopFlowProcess) {
//            return ((HadoopFlowProcess)getDelegate()).getJobConf();
//        } else {
//            return new JobConf();
//        }
//    }
//
    public void setStatus(String msg) {
        setStatus(Level.SLF4J_INFO, msg);
    }

    public void setStatus(String msg, Throwable t) {
        super.setStatus(msg);
        
        for (IFlowReporter reporter : _reporters) {
            reporter.setStatus(msg, t);
        }
        
        increment(LoggingLevels.ERROR, 1);
    }

    public void setStatus(Level level, String msg) {
        super.setStatus(msg);
        
        for (IFlowReporter reporter : _reporters) {
            reporter.setStatus(level, msg);
        }
        
        increment(LoggingLevels.fromLevel(level), 1);
    }

    @Override
    public void increment(Enum counter, long amount) {
        super.increment(counter, amount);

        // TODO KKr - decide if I really want to track stuff locally
        if (true || _isLocal) {
            synchronized (_localCounters) {
                if (_localCounters.get(counter) == null) {
                    _localCounters.put(counter, new AtomicLong());
                }
            }

            AtomicLong curCount = _localCounters.get(counter);
            long newValue = curCount.addAndGet(amount);

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Cascading counter: " + counter + (amount > 0 ? " + " : " - ")
                                + Math.abs(amount) + " = " + newValue);
            }
        }
    }

    @Override
    public void increment(String group, String counter, long amount) {
        super.increment(group, counter, amount);
        
        // TODO KKr - get my local counters in sync?
    }

    public void decrement(Enum counter, long amount) {
        increment(counter, -amount);
    }

    /**
     * @param counter whose value should be returned
     * @return current value of the counter, local to the task
     * <br/><br/><b>Note:</b> Only the JobTracker aggregates task counter values
     * to report the job-wide total.
     */
    public long getCounter(Enum counter) {
        if (_isLocal) {
            AtomicLong count = _localCounters.get(counter);
            if (count != null) {
                return count.get();
            } else {
                return 0;
            }
        } else {
            Counters counters = new Counters();
            Counter hadoopCounter = counters.findCounter(counter);
            if (hadoopCounter != null) {
                return (int)hadoopCounter.getValue();
            } else {
                return 0;
            }
        }
    }

    /**
     * If we're running in local mode, log current counter values.
     */
    public void dumpCounters() {
        if (_isLocal) {
            for (Enum theEnum : _localCounters.keySet()) {
                LOGGER.info(String.format("Cascading counter: %s = %d", theEnum, _localCounters
                                .get(theEnum).get()));
            }
        }

        // FUTURE KKr - also dump Hadoop counters to Logger?
    }

}
