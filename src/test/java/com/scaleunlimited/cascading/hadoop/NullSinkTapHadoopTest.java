/**
 * Copyright 2010-2012 TransPac Software, Inc.
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

package com.scaleunlimited.cascading.hadoop;

import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.scaleunlimited.cascading.NullSinkTap;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowStep;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.HadoopFlowStep;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class NullSinkTapHadoopTest {
    
    @Test
    public void testNullSinkTap() throws IOException {
        Lfs in = new Lfs(new SequenceFile(new Fields("input")), "build/test/NullSinkTapHadoopTest/testNullSinkTap/in", SinkMode.REPLACE);
        TupleEntryCollector write = in.openForWrite(new HadoopFlowProcess());
        Tuple tuple = new Tuple("value");
        write.add(tuple);
        write.close();

        NullSinkTap out = new NullSinkTap(new Fields("input"));

        Pipe pipe = new Pipe("pipe");
        pipe = new Each("pipe", new Identity());
        
        FlowConnector flowConnector = new HadoopFlowConnector();
        flowConnector.connect(in, out, pipe).complete();
    }
    
    @Test
    public void testNullSinkTapWrongOutputFields() throws IOException {
        final Fields sourceFields = new Fields("input");
        Lfs in = new Lfs(new SequenceFile(sourceFields), "build/test/NullSinkTapHadoopTest/testNullSinkTapWrongOutputFields/in", SinkMode.REPLACE);
        TupleEntryCollector write = in.openForWrite(new HadoopFlowProcess());
        Tuple tuple = new Tuple("value");
        write.add(tuple);
        write.close();

        // Set up output where it's got an extra field
        final Fields sinkFields = new Fields("input", "bogus");
        NullSinkTap out = new NullSinkTap(sinkFields);

        Pipe pipe = new Pipe("pipe");
        pipe = new Each("pipe", new Identity());
        
        FlowConnector flowConnector = new HadoopFlowConnector();
        
        try {
            flowConnector.connect(in, out, pipe);
            Assert.fail("Should have thrown an exception");
        } catch (Exception e) {
            // expected
        }
    }
    
    @Test
    public void testNumSinkParts() throws Exception {
        final Fields fields = new Fields("input");
        Lfs in = new Lfs(new SequenceFile(fields), "build/test/NullSinkTapHadoopTest/testNumSinkParts/in", SinkMode.REPLACE);
        TupleEntryCollector write = in.openForWrite(new HadoopFlowProcess());
        for (int i = 0; i < 100; i++) {
            Tuple tuple = new Tuple("value-" + i);
            write.add(tuple);
        }
        write.close();

        // Create tap with 3 parts
        final int numSinkParts = 3;
        NullSinkTap out = new NullSinkTap(fields, numSinkParts);

        Pipe pipe = new Pipe("pipe");
        pipe = new GroupBy(pipe, fields);
        pipe = new Each(pipe, new Identity());
        
        FlowConnector flowConnector = new HadoopFlowConnector();
        Flow f = flowConnector.connect(in, out, pipe);
        List<HadoopFlowStep> steps = f.getFlowSteps();
        Assert.assertEquals(1, steps.size());
        HadoopFlowStep step = steps.get(0);
        Assert.assertEquals(numSinkParts, step.getConfig().getNumReduceTasks());
        
        // Make sure the job actually runs
        f.complete();
    }
    
    @Test
    public void testNullSinkTapNoFields() throws IOException {
        Lfs in = new Lfs(new SequenceFile(new Fields("input")), "build/test/NullSinkTapHadoopTest/testNullSinkTapNoFields/in", SinkMode.REPLACE);
        TupleEntryCollector write = in.openForWrite(new HadoopFlowProcess());
        Tuple tuple = new Tuple("value");
        write.add(tuple);
        write.close();

        NullSinkTap out = new NullSinkTap();

        Pipe pipe = new Pipe("pipe");
        pipe = new Each("pipe", new Identity());
        
        FlowConnector flowConnector = new HadoopFlowConnector();
        flowConnector.connect(in, out, pipe).complete();
    }
    
	
}
