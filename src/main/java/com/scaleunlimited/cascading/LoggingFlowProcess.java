/**
 * Copyright 2010 TransPac Software, Inc.
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

import cascading.flow.FlowProcess;
import cascading.flow.FlowProcessWrapper;
import cascading.flow.hadoop.HadoopFlowProcess;

public class LoggingFlowProcess<Config> extends FlowProcessWrapper<Config> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingFlowProcess.class);

    // TODO VMa: fix for slf4j
    // Why do we need this enum ?
    public static enum LoggingLevels {
        TRACE,
        DEBUG,
        INFO,
        WARN,
        ERROR,
        FATAL,
        EXCEPTION;
        
        public static LoggingLevels fromLevel(Level level) {
            switch (level.toInt()) {
                case Level.TRACE_INT: return TRACE;
                case Level.DEBUG_INT: return DEBUG;
                case Level.ERROR_INT: return ERROR;
                case Level.FATAL_INT: return FATAL;
                case Level.INFO_INT: return INFO;
                case Level.WARN_INT: return WARN;
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
            if ((_reporter != null) && level.isGreaterOrEqual(Level.INFO)) {
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
        init(baseProcess, reporter);
    }

    public LoggingFlowProcess(FlowProcess<Config> baseProcess) {
        super(baseProcess);
        
        if (baseProcess instanceof HadoopFlowProcess) {
            init(baseProcess, new HadoopFlowReporter(((HadoopFlowProcess)baseProcess).getReporter()));
        } else {
            init(baseProcess, new LoggingFlowReporter());
        }
    }
    
    public LoggingFlowProcess(HadoopFlowProcess baseProcess) {
        super(baseProcess);
        
        IFlowReporter reporter = new HadoopFlowReporter(baseProcess.getReporter());
        init(baseProcess, reporter);
    }

    /**
     * A no-argument constructor for use during testing, when we don't have a
     * real Cascading FlowProcess to use.
     */
    public LoggingFlowProcess() {
        super(FlowProcess.NULL);
        
        init(FlowProcess.NULL, new LoggingFlowReporter());
    }

    private void init(FlowProcess baseProcess, IFlowReporter reporter) {
        _isLocal = !(baseProcess instanceof HadoopFlowProcess)
                        || ((HadoopFlowProcess) baseProcess).getJobConf().get("mapred.job.tracker")
                                        .equalsIgnoreCase("local");

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
        
        // TODO VMa: fix for slf4j
        increment(LoggingLevels.EXCEPTION, 1);
    }

    public void setStatus(Level level, String msg) {
        super.setStatus(msg);
        
        for (IFlowReporter reporter : _reporters) {
            reporter.setStatus(level, msg);
        }
        
        // TODO VMa: fix for slf4j
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
