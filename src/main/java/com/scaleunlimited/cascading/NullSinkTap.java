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
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.SinkTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.util.Util;

@SuppressWarnings({ "serial" })
public class NullSinkTap extends SinkTap<Object, Object> {
	
    private static class NullScheme extends Scheme<Object, Object, Object, Object, Object> {
        
        public NullScheme() {
            super();
        }
        
        public NullScheme(Fields fields) {
            super(Fields.UNKNOWN, fields);
        }
        
        @Override
        public boolean isSink() {
            return true;
        }
        
        @Override
        public boolean isSource() {
            return false;
        }
        
        @Override
        public void sink(FlowProcess<Object> flowProcess, SinkCall<Object, Object> sinkCall) throws IOException {
        }

        @Override
        public void sinkConfInit(FlowProcess<Object> flowProcess, Tap<Object, Object, Object> tap, Object properties) {
        }

        @Override
        public boolean source(FlowProcess<Object> flowProcess, SourceCall<Object, Object> sourceCall) throws IOException {
            throw new UnsupportedOperationException("NullSinkTap can only be used as a sink, not a source");
        }

        @Override
        public void sourceConfInit(FlowProcess<Object> flowProcess, Tap<Object, Object, Object> tap, Object properties) {
            throw new UnsupportedOperationException("NullSinkTap can only be used as a sink, not a source");
        }
        
    }
    
    private String _id;
    
	public NullSinkTap(Fields fields) {
        super(new NullScheme(fields));
        _id = makeUniqueId();
    }

    public NullSinkTap() {
        super(new NullScheme());
        _id = makeUniqueId();
    }

    private String makeUniqueId() {
        return "NullSinkTap-" + Util.createUniqueID();
    }
    
    @Override
    public boolean createResource(Object properties) throws IOException {
        return true;
    }

    @Override
    public boolean deleteResource(Object properties) throws IOException {
        return false;
    }

    @Override
    public String getIdentifier() {
        return _id;
    }

    @Override
    public boolean resourceExists(Object properties) throws IOException {
        return true;
    }

    @Override
    public long getModifiedTime(Object properties) throws IOException {
        return 0;
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Object> flowProcess, Object out) throws IOException {
        return new TupleEntrySchemeCollector<Object, OutputStream>(flowProcess, getScheme(), new OutputStream() {

            @Override
            public void write(int b) throws IOException { }
        });
    }

}
