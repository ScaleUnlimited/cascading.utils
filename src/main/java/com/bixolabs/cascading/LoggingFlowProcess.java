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

package com.bixolabs.cascading;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

@SuppressWarnings("unchecked")
public class LoggingFlowProcess extends FlowProcess {
    private static final Logger LOGGER = Logger.getLogger(LoggingFlowProcess.class);

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
    
    // TODO KKr - extend HadoopFlowProces, use Reporter.NULL for reporter, etc.
    // would
    // be safer than relying on never casting this to a HadoopFlowProcess when
    // in local
    // mode, which is fragile. Though better might be for FlowProcess to support
    // an isLocal() call, and a getReporter() call, where it returns null for
    // getReporter
    // when running local or if it's not a HadoopFlowProcess.
    private class FakeFlowProcess extends FlowProcess {

        @Override
        public Object getProperty(String key) {
            return null;
        }

        @Override
        public void increment(Enum counter, int amount) {
        }

        @Override
        public void increment(String group, String counter, int amount) {
        }
        
        @Override
        public void keepAlive() {
        }

        @Override
        public TupleEntryIterator openTapForRead(Tap tap) throws IOException {
            return null;
        }

        @Override
        public TupleEntryCollector openTapForWrite(Tap tap) throws IOException {
            return null;
        }

        @Override
        public void setStatus(String status) {
        }
    }

    private class HadoopFlowReporter implements IFlowReporter {
        private Reporter _reporter;

        public HadoopFlowReporter(Reporter reporter) {
            _reporter = reporter;
        }

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
                _reporter.setStatus("Cascading " + Level.ERROR + ": " + msg);
            }
        }
    }

    private FlowProcess _baseProcess;
    private boolean _isLocal;
    private List<IFlowReporter> _reporters;
    private Map<Enum, AtomicInteger> _localCounters;

    public LoggingFlowProcess(FlowProcess baseProcess, IFlowReporter reporter) {
        init(baseProcess, reporter);
    }

    public LoggingFlowProcess(FlowProcess baseProcess) {
        if (baseProcess instanceof HadoopFlowProcess) {
            init(baseProcess, new HadoopFlowReporter(((HadoopFlowProcess)baseProcess).getReporter()));
        } else {
            init(baseProcess, new LoggingFlowReporter());
        }
    }
    
    public LoggingFlowProcess(HadoopFlowProcess baseProcess) {
        IFlowReporter reporter = new HadoopFlowReporter(baseProcess.getReporter());
        init(baseProcess, reporter);
    }

    /**
     * A no-argument constructor for use during testing, when we don't have a
     * real Cascading FlowProcess to use.
     */
    public LoggingFlowProcess() {
        init(new FakeFlowProcess(), new LoggingFlowReporter());
    }

    private void init(FlowProcess baseProcess, IFlowReporter reporter) {
        _baseProcess = baseProcess;
        _isLocal = !(baseProcess instanceof HadoopFlowProcess)
                        || ((HadoopFlowProcess) baseProcess).getJobConf().get("mapred.job.tracker")
                                        .equalsIgnoreCase("local");

        _localCounters = new HashMap<Enum, AtomicInteger>();
        _reporters = new ArrayList<IFlowReporter>();
        addReporter(reporter);
    }

    public void addReporter(IFlowReporter reporter) {
        _reporters.add(reporter);
    }
    
    @SuppressWarnings("deprecation")
    public JobConf getJobConf() throws IOException {
        if (_baseProcess instanceof HadoopFlowProcess) {
            return ((HadoopFlowProcess)_baseProcess).getJobConf();
        } else {
            return new JobConf();
        }
    }

    public void setStatus(String msg) {
        setStatus(Level.INFO, msg);
    }

    public void setStatus(String msg, Throwable t) {
        for (IFlowReporter reporter : _reporters) {
            reporter.setStatus(msg, t);
        }
        
        increment(LoggingLevels.EXCEPTION, 1);
    }

    public void setStatus(Level level, String msg) {
        for (IFlowReporter reporter : _reporters) {
            reporter.setStatus(level, msg);
        }
        
        increment(LoggingLevels.fromLevel(level), 1);
    }

    @Override
    public void increment(Enum counter, int amount) {
        _baseProcess.increment(counter, amount);

        // TODO KKr - decide if I really want to track stuff locally
        if (true || _isLocal) {
            synchronized (_localCounters) {
                if (_localCounters.get(counter) == null) {
                    _localCounters.put(counter, new AtomicInteger());
                }
            }

            AtomicInteger curCount = _localCounters.get(counter);
            int newValue = curCount.addAndGet(amount);

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Cascading counter: " + counter + (amount > 0 ? " + " : " - ")
                                + Math.abs(amount) + " = " + newValue);
            }
        }
    }

    @Override
    public void increment(String group, String counter, int amount) {
        _baseProcess.increment(group, counter, amount);
        
        // TODO KKr - get my local counters in sync?
    }

    public void decrement(Enum counter, int amount) {
        increment(counter, -amount);
    }

    /**
     * @param counter whose value should be returned
     * @return current value of the counter, local to the task
     * <br/><br/><b>Note:</b> Only the JobTracker aggregates task counter values
     * to report the job-wide total.
     */
    public int getCounter(Enum counter) {
        if (_isLocal) {
            AtomicInteger count = _localCounters.get(counter);
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

    @Override
    public Object getProperty(String key) {
        return _baseProcess.getProperty(key);
    }

    @Override
    public void keepAlive() {
        _baseProcess.keepAlive();
    }

    @Override
    public TupleEntryIterator openTapForRead(Tap tap) throws IOException {
        return _baseProcess.openTapForRead(tap);
    }

    @Override
    public TupleEntryCollector openTapForWrite(Tap tap) throws IOException {
        return _baseProcess.openTapForWrite(tap);
    }


}
