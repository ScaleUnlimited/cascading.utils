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

package com.bixolabs.cascading.local;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import com.bixolabs.cascading.NullContext;
import com.bixolabs.cascading.NullSinkTap;

import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class NullSinkTapLocalTest {
    
    @Test
    public void testNullSinkTap() throws IOException {
        FileTap in = makeSourceData("testNullSinkTap");
        NullSinkTap out = new NullSinkTap(new Fields("input"));

        Pipe pipe = new Pipe("pipe");
        pipe = new Each("pipe", new Identity());
        
        FlowConnector flowConnector = new LocalFlowConnector();
        flowConnector.connect(in, out, pipe).complete();
    }
    
    private FileTap makeSourceData(String dirName) throws IOException {
        
        File inputDir = new File("build/test/NullSinkTapLocalTest/" + dirName + "/in");
        inputDir.mkdirs();
        
        File inputFile = new File(inputDir, "source-file");
        inputFile.createNewFile();
        
        FileTap in = new FileTap(new TextLine(new Fields("input")), inputFile.getAbsolutePath(), SinkMode.REPLACE);
        TupleEntryCollector write = in.openForWrite(new LocalFlowProcess());
        Tuple tuple = new Tuple("value");
        write.add(tuple);
        write.close();
        
        return in;
    }

    @Test
    public void testNullSinkTapWrongOutputFields() throws IOException {
        FileTap in = makeSourceData("testNullSinkTapWrongOutputFields");

        // Set up output where it's got an extra field
        NullSinkTap out = new NullSinkTap(new Fields("input", "missing-bogus"));

        Pipe pipe = new Pipe("pipe");
        pipe = new Each("pipe", new Identity());
        
        FlowConnector flowConnector = new LocalFlowConnector();
        
        try {
            flowConnector.connect(in, out, pipe);
            Assert.fail("Should have thrown an exception");
        } catch (Exception e) {
            // expected
        }
    }
    
    @Test
    public void testNullSinkTapNoFields() throws IOException {
        FileTap in = makeSourceData("testNullSinkTapNoFields");
        NullSinkTap out = new NullSinkTap();

        Pipe pipe = new Pipe("pipe");
        pipe = new Each("pipe", new Identity());
        
        FlowConnector flowConnector = new LocalFlowConnector();
        flowConnector.connect(in, out, pipe).complete();
    }
    
	
}
