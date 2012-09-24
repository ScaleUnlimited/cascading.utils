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

package com.scaleunlimited.cascading;

import java.io.IOException;
import java.io.OutputStream;

import cascading.flow.FlowProcess;
import cascading.tap.SinkTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntrySchemeCollector;

@SuppressWarnings({ "serial" })
public class NullSinkTap extends SinkTap<NullContext, Object> {
	
	public NullSinkTap(Fields sourceFields) {
        super(new NullScheme<NullContext>(sourceFields));
    }

    public NullSinkTap() {
        super(new NullScheme<NullContext>());
    }

    @Override
    public boolean createResource(NullContext properties) throws IOException {
        return true;
    }

    @Override
    public boolean deleteResource(NullContext properties) throws IOException {
        return false;
    }

    @Override
    public String getIdentifier() {
        return "null";
    }

    @Override
    public long getModifiedTime(NullContext properties) throws IOException {
        return 0;
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<NullContext> flowProcess, Object out) throws IOException {
        return new TupleEntrySchemeCollector<NullContext, OutputStream>(flowProcess, getScheme(), new OutputStream() {

            @Override
            public void write(int b) throws IOException { }
        });
    }

    @Override
    public boolean resourceExists(NullContext properties) throws IOException {
        return true;
    }

}
