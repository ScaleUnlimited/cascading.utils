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

import java.io.File;
import java.io.FileReader;

import junit.framework.Assert;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.FlowStep;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.planner.BaseFlowStep;
import cascading.operation.BaseOperation;
import cascading.operation.Debug;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.aggregator.Sum;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.stats.FlowStepStats;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class FlowMonitorTest {

    private enum MyCounters {
        FILTER_REQUESTS,
    }
    
    @SuppressWarnings({ "serial", "unchecked" })
    private static class MyFilter extends BaseOperation implements Filter {

        private int _delay;
        
        @SuppressWarnings("unused")
        public MyFilter() {
            this(0);
        }
        
        public MyFilter(int delay) {
            _delay = delay;
        }
        
        @Override
        public boolean isRemove(FlowProcess process, FilterCall filterCall) {
            process.increment(MyCounters.FILTER_REQUESTS, 1);
            
            if (_delay > 0) {
                try {
                    Thread.sleep(_delay);
                } catch (InterruptedException e) {
                    // Do nothing
                }
            }
            
            return false;
        }
        
        @Override
        public String toString() {
            return "Filter stuff";
        }
    }
    
    private static class MyTask implements IMonitorTask {

        @Override
        public String getName(Flow flow, FlowStep flowStep) {
            return "MyTask";
        }

        @Override
        public String getValue(Flow flow, FlowStep flowStep, FlowStepStats stepStats) {
            return "200 bytes/sec & faster!";
        }
        
    }
    
    @Test
    public void testHtmlGeneration() throws Throwable {
        final Fields groupField = new Fields("user");
        final Fields testFields = new Fields("user", "value");
        
        final int perRequestDelay = 0;
        final int numDatums = 100;
        
        final String testDir = "build/test/FlowMonitorTest/testHtmlGeneration/";
        String in = testDir + "in";
        String out = testDir + "out";

        Lfs sourceTap = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
        TupleEntryCollector write = sourceTap.openForWrite(new HadoopFlowProcess());
        
        for (int i = 0; i < numDatums; i++) {
            String username = "user-" + (i % 3);
            write.add(new Tuple(username, i));
        }
        
        write.close();

        Pipe pipe = new Pipe("test");
        pipe = new Each(pipe, new Debug());
        pipe = new GroupBy("group by user", pipe, groupField);
        pipe = new Every(pipe, new Fields("value"), new Sum());
        pipe = new Each(pipe, new MyFilter(perRequestDelay));
        pipe = new GroupBy("group by sum", pipe, new Fields("sum"));
        Lfs sinkTap = new Lfs(new SequenceFile(new Fields("user", "sum")), out, SinkMode.REPLACE);
        
        Flow<JobConf> flow = new HadoopFlowConnector().connect("FlowMonitorTest", sourceTap, sinkTap, pipe);
        for (FlowStep<JobConf> step : flow.getFlowSteps()) {
            StepUtils.nameFlowStep((BaseFlowStep<JobConf>)step);
        }

        FlowMonitor monitor = new FlowMonitor(flow);
        monitor.setUpdateInterval(100);
        monitor.setHtmlDirectory(testDir);
        monitor.addMonitorTask(new MyTask());
        monitor.setIncludeCascadingCounters(true);
        
        Assert.assertTrue(monitor.run(MyCounters.FILTER_REQUESTS));
        
        File htmlFile = new File(testDir + FlowMonitor.FILENAME);
        Assert.assertTrue(htmlFile.exists());
        String content = IOUtils.toString(new FileReader(htmlFile));
        Assert.assertTrue(content.contains("FlowMonitorTest"));
        Assert.assertTrue(content.contains(MyCounters.FILTER_REQUESTS.toString()));
        Assert.assertTrue(content.contains("<td>MyTask</td>"));
        Assert.assertTrue(content.contains("200 bytes/sec &amp; faster!"));
        Assert.assertTrue(content.contains("<td>" + numDatums + "</td>"));
        
        // Make sure we're not getting negative durations
        Assert.assertFalse(content.contains("<td>-"));
    }
    
    @Test
    public void testArchivingFiles() throws Throwable {
        final Fields testFields = new Fields("user", "value");
        
        final int numDatums = 1;
        
        final String testDir = "build/test/FlowMonitorTest/testArchivingFiles/";
        String in = testDir + "in";
        String out = testDir + "out";

        Lfs sourceTap = new Lfs(new SequenceFile(testFields), in, SinkMode.REPLACE);
        TupleEntryCollector write = sourceTap.openForWrite(new HadoopFlowProcess());
        
        for (int i = 0; i < numDatums; i++) {
            String username = "user-" + (i % 3);
            write.add(new Tuple(username, i));
        }
        
        write.close();

        Pipe pipe = new Pipe("test");
        Lfs sinkTap = new Lfs(new SequenceFile(testFields), out, SinkMode.REPLACE);
        
        final String flowName = "testArchivingFiles";
        Flow<JobConf> flow = new HadoopFlowConnector().connect(flowName, sourceTap, sinkTap, pipe);
        for (FlowStep<JobConf> step : flow.getFlowSteps()) {
            StepUtils.nameFlowStep((BaseFlowStep<JobConf>)step);
        }

        FlowMonitor monitor = new FlowMonitor(flow);
        monitor.setUpdateInterval(100);
        monitor.setHtmlDirectory(testDir);
        monitor.setIncludeCascadingCounters(true);
        Assert.assertTrue(monitor.run());

        File archiveFile = new File(testDir + flowName + "-" + FlowMonitor.FILENAME);
        Assert.assertTrue(archiveFile.exists());
        String content = IOUtils.toString(new FileReader(archiveFile));
        Assert.assertTrue(content.contains(flowName));
        
        // TODO this doesn't work - we don't get any counters showing up here
        // Assert.assertTrue(content.contains("<td>" + numDatums + "</td>"));
    }
}
