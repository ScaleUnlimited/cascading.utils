package com.scaleunlimited.cascading;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowStep;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;

import com.scaleunlimited.cascading.hadoop.HadoopPlatform;
import com.scaleunlimited.cascading.local.LocalPlatform;

public class FlowUtilsTest {

    private static final String FLOW_NAME = "FlowUtilsTest";
    
    @Test
    public void testNamingStepsForMapOnlyJob() throws Exception {
        String stepName = "Debug (1/1) /path/to/dest";
        JobConf conf = (JobConf)testNamingStepsForMapOnlyJob(new HadoopPlatform(this.getClass()), stepName);
        assertEquals(FLOW_NAME + "/" + stepName, conf.getJobName());
        testNamingStepsForMapOnlyJob(new LocalPlatform(this.getClass()), "Debug local");
    }

    private Object testNamingStepsForMapOnlyJob(BasePlatform platform, String expectedStepName) throws Exception {
        Pipe p = new Pipe("map-only");
        p = new Each(p, new Debug());
        
        return makeAndNameFlow(platform, expectedStepName, p);
    }

    @Test
    public void testNamingStepsForGroupingJob() throws Exception {
        String stepName = "grouping (1/1) /path/to/dest";
        JobConf conf = (JobConf)testNamingStepsForGroupingJob(new HadoopPlatform(this.getClass()), stepName);
        assertEquals(FLOW_NAME + "/" + stepName, conf.getJobName());
        testNamingStepsForGroupingJob(new LocalPlatform(this.getClass()), "grouping local");
    }
    
    private Object testNamingStepsForGroupingJob(BasePlatform platform, String expectedStepName) throws Exception {
        Pipe p = new Pipe("grouping");
        p = new GroupBy("grouping", p, new Fields("key"));
        
        return makeAndNameFlow(platform, expectedStepName, p);
    }

    private Object makeAndNameFlow(BasePlatform platform, String expectedStepName, Pipe p) throws Exception {
        FlowDef flowDef = new FlowDef()
            .setName(FLOW_NAME)
            .addSource(p, platform.makeTap(platform.makeBinaryScheme(new Fields("key")), platform.makePath("/path/to/source")))
            .addTailSink(p, platform.makeTap(platform.makeBinaryScheme(new Fields("key")), platform.makePath("/path/to/dest"), SinkMode.REPLACE));
        Flow f = platform.makeFlowConnector().connect(flowDef);
        FlowUtils.nameFlowSteps(f);
    
        List<FlowStep> steps = f.getFlowSteps();
        assertEquals(1, steps.size());
        FlowStep fs = steps.get(0);
        assertEquals(expectedStepName, fs.getName());
    
        return fs.getConfig();
    }
    
    
}
