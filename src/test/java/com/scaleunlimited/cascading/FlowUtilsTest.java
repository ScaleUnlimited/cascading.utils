package com.scaleunlimited.cascading;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
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

    @Test
    public void testNamingStepsForMapOnlyJob() throws Exception {
        testNamingStepsForMapOnlyJob(new HadoopPlatform(FlowUtilsTest.class));
        testNamingStepsForMapOnlyJob(new LocalPlatform(FlowUtilsTest.class));
    }

    private void testNamingStepsForMapOnlyJob(BasePlatform platform) throws Exception {
        Pipe p = new Pipe("map-only");
        p = new Each(p, new Debug());
        
        FlowConnector fc = platform.makeFlowConnector();
        Flow f = fc.connect(platform.makeTap(platform.makeBinaryScheme(new Fields("key")), platform.makePath("/path/to/source")), 
                            platform.makeTap(platform.makeBinaryScheme(new Fields("key")), platform.makePath("/path/to/dest"), SinkMode.REPLACE),
                            p);
        
        FlowUtils.nameFlowSteps(f);
        
        List<FlowStep> steps = f.getFlowSteps();
        assertEquals(1, steps.size());
        
        assertEquals("Debug", steps.get(0).getName());
    }

    @Test
    public void testNamingStepsForGroupingJob() throws Exception {
        testNamingStepsForGroupingJob(new HadoopPlatform(FlowUtilsTest.class));
        testNamingStepsForGroupingJob(new LocalPlatform(FlowUtilsTest.class));
    }
    
    private void testNamingStepsForGroupingJob(BasePlatform platform) throws Exception {
        Pipe p = new Pipe("grouping");
        p = new GroupBy("grouping", p, new Fields("key"));
        
        FlowConnector fc = platform.makeFlowConnector();
        Flow f = fc.connect(platform.makeTap(platform.makeBinaryScheme(new Fields("key")), platform.makePath("/path/to/source")), 
                            platform.makeTap(platform.makeBinaryScheme(new Fields("key")), platform.makePath("/path/to/dest"), SinkMode.REPLACE),
                            p);
        
        FlowUtils.nameFlowSteps(f);
        
        List<FlowStep> steps = f.getFlowSteps();
        assertEquals(1, steps.size());
        
        assertEquals("grouping", steps.get(0).getName());
    }

    
    
}
