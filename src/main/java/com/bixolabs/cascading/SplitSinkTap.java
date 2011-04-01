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

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

import cascading.scheme.Scheme;
import cascading.scheme.SequenceFile;
import cascading.tap.CompositeTap;
import cascading.tap.SinkTap;
import cascading.tap.Tap;
import cascading.tap.hadoop.MultiInputFormat;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings("serial")
public class SplitSinkTap extends SinkTap implements CompositeTap {
    private static final Logger LOGGER = Logger.getLogger(SplitSinkTap.class);

    private BaseSinkSplitter _splitter;
    private Map<String, Tap> _taps;
    
    public SplitSinkTap(Map<String, Tap> taps, BaseSinkSplitter splitter) {
        _taps = taps;
        _splitter = splitter;
    }

    private String tempPath = "__splitsink_placeholder" + Integer.toString((int)(System.currentTimeMillis() * Math.random()));
    private List<Map<String, String>> childConfigs;

    private class SplitSinkCollector extends TupleEntryCollector implements OutputCollector {
        OutputCollector[] collectors;

        public SplitSinkCollector(JobConf conf, Tap... taps) throws IOException {
            collectors = new OutputCollector[taps.length];

            conf = new JobConf(conf);

            JobConf[] jobConfs = MultiInputFormat.getJobConfs(conf, childConfigs);

            for (int i = 0; i < taps.length; i++) {
                Tap tap = taps[i];
                LOGGER.info("opening for write: " + tap.toString());

                collectors[i] = (OutputCollector) tap.openForWrite(jobConfs[i]);
            }
        }

        protected void collect(Tuple tuple) {
            throw new UnsupportedOperationException("collect should never be called on MultiSinkCollector");
        }

        public void collect(Object key, Object value) throws IOException {
            for (OutputCollector collector : collectors)
                collector.collect(key, value);
        }

        @Override
        public void close() {
            super.close();

            try {
                for (OutputCollector collector : collectors) {
                    try {
                        ((TupleEntryCollector) collector).close();
                    } catch (Exception exception) {
                        LOGGER.warn("exception closing TupleEntryCollector", exception);
                    }
                }
            } finally {
                collectors = null;
            }
        }
    }

    protected Tap[] getTaps() {
        return _taps.values().toArray(new Tap[_taps.size()]);
    }

    @Override
    public Tap[] getChildTaps() {
        return getTaps();
    }

    @Override
    public boolean isWriteDirect() {
        return true;
    }

    @Override
    public Path getPath() {
        return new Path(tempPath);
    }

    @SuppressWarnings("deprecation")
    @Override
    public TupleEntryCollector openForWrite(JobConf conf) throws IOException {
        // TODO KKr - why do we need a SplitSinkCollector
        return new SplitSinkCollector(conf, getTaps());
    }

    @SuppressWarnings("deprecation")
    @Override
    public void sinkInit(JobConf conf) throws IOException {
        childConfigs = new ArrayList<Map<String, String>>();

        for (Tap tap : getTaps()) {
            JobConf jobConf = new JobConf(conf);
            tap.sinkInit(jobConf);
            
            // TODO KKr - why do we need childConfigs?
            childConfigs.add(MultiInputFormat.getConfig(conf, jobConf));
        }
    }

    @Override
    public boolean makeDirs(JobConf conf) throws IOException {
        for (Tap tap : getTaps()) {
            if (!tap.makeDirs(conf))
                return false;
        }

        return true;
    }

    @Override
    public boolean deletePath(JobConf conf) throws IOException {
        for (Tap tap : getTaps()) {
            if (!tap.deletePath(conf))
                return false;
        }

        return true;
    }

    @Override
    public boolean pathExists(JobConf conf) throws IOException {
        for (Tap tap : getTaps()) {
            if (!tap.pathExists(conf))
                return false;
        }

        return true;
    }

    @SuppressWarnings("deprecation")
    @Override
    public long getPathModified(JobConf conf) throws IOException {
        long modified = Long.MIN_VALUE;
        for (Tap tap : getTaps()) {
            modified = Math.max(modified, tap.getPathModified(conf));
        }
        
        return modified;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
        String key = _splitter.getSplitKey(tupleEntry);
        Tap tap = _taps.get(key);
        if (tap != null) {
            tap.sink(tupleEntry, outputCollector);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Scheme getScheme() {
        if (super.getScheme() != null) {
            return super.getScheme();
        }

        Set<Comparable> fieldNames = new LinkedHashSet<Comparable>();
        for (Tap tap : getTaps()) {
            for (Object o : tap.getSinkFields()) {
                fieldNames.add((Comparable) o);
            }
        }

        Fields allFields = new Fields(fieldNames.toArray(new Comparable[fieldNames.size()]));

        setScheme(new SequenceFile(allFields));

        return super.getScheme();
    }

    @Override
    public String toString() {
        return "SplitSinkTap[" + Arrays.asList(getTaps()) + ']';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof SplitSinkTap))
            return false;
        if (!super.equals(o))
            return false;

        SplitSinkTap that = (SplitSinkTap) o;

        if (!Arrays.equals(getTaps(), that.getTaps()))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Arrays.hashCode(getTaps());
        return result;
    }

}
