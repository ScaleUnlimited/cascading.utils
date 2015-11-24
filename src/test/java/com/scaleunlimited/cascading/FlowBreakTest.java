package com.scaleunlimited.cascading;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;

public class FlowBreakTest {

    private static Flow makeFlow(boolean useCheckpoint) {
        
        Pipe head = new Pipe("pipe");
        head = new Each(head, new Identity());
        
        if (useCheckpoint) {
            head = new FlowBreak(head);
        }
        
        Pipe tail1 = new Pipe("tail1", head);
        tail1 = new Each(tail1, new Identity());
        
        Pipe tail2 = new Pipe("tail2", head);
        tail2 = new Each(tail2, new Identity());

        Map<String, Tap> sinks = new HashMap<String, Tap>();
        sinks.put(tail1.getName(), new Lfs(new TextLine(), "dest/path1"));
        sinks.put(tail2.getName(), new Lfs(new TextLine(), "dest/path2"));
        
        FlowConnector fc = new Hadoop2MR1FlowConnector();
        return fc.connect(new Lfs(new TextLine(), "source/path"), sinks, tail1, tail2);
    }
    
    @Test
    public void test() {
        // Create a simple flow, and check the number of steps
        File destDir = new File("build/test/CheckpointTest/");
        destDir.mkdirs();
        
        Flow f1 = makeFlow(false);
        // f1.writeDOT("build/test/CheckpointTest/f1.dot");
        
        Flow f2 = makeFlow(true);
        // f2.writeDOT("build/test/CheckpointTest/f2.dot");
        
        assertTrue(f1.getFlowSteps().size() < f2.getFlowSteps().size());
    }

}
