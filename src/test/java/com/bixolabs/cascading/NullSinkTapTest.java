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

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class NullSinkTapTest {
	@Test
	public void testNullSinkTap() throws IOException {
        Lfs in = new Lfs(new SequenceFile(new Fields("input")), "build/test/NullSinkTapTest/in", true);
        TupleEntryCollector write = in.openForWrite(new JobConf());
        Tuple tuple = new Tuple("value");
        write.add(tuple);
        write.close();

        Tap out = new NullSinkTap(new Fields("input"));

        Pipe pipe = new Pipe("pipe");
        pipe = new Each("pipe", new Identity());
        
        FlowConnector flowConnector = new FlowConnector();
        Flow flow = flowConnector.connect(in, out, pipe);
        flow.complete();
	}
}
