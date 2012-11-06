package com.scaleunlimited.cascading;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowStep;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;

public class BaseToolTest extends BaseTool {

    @Test
    public void testNamingStepsForMapOnlyJob() {
        Pipe p = new Pipe("map-only");
        p = new Each(p, new Debug());
        
        FlowConnector fc = new HadoopFlowConnector();
        Flow f = fc.connect(new Lfs(new Fields("key"), "/path/to/source"), new Lfs(new Fields("key"), "/path/to/dest"), p);
        
        nameFlowSteps(f);
        
        List<FlowStep> steps = f.getFlowSteps();
        assertEquals(1, steps.size());
        
        assertEquals("Debug", steps.get(0).getName());
    }

    @Test
    public void testNamingStepsForGroupingJob() {
        Pipe p = new Pipe("map-only");
        p = new GroupBy("grouping", p, new Fields("key"));
        
        FlowConnector fc = new HadoopFlowConnector();
        Flow f = fc.connect(new Lfs(new Fields("key"), "/path/to/source"), new Lfs(new Fields("key"), "/path/to/dest"), p);
        
        nameFlowSteps(f);
        
        List<FlowStep> steps = f.getFlowSteps();
        assertEquals(1, steps.size());
        
        assertEquals("grouping", steps.get(0).getName());
    }

}
