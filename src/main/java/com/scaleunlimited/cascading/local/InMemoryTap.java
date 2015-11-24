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

package com.scaleunlimited.cascading.local;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.tuple.TupleEntrySchemeIterator;

import com.scaleunlimited.cascading.NullContext;

@SuppressWarnings("serial")
public class InMemoryTap extends Tap<Properties, InputStream, OutputStream> {

    private static class CloseableList<T> implements List<T>, Closeable {
        
        private List<T> _list;

        public CloseableList(List<T> list) {
            _list = list;
        }
        
        public int size() {
            return _list.size();
        }

        public boolean isEmpty() {
            return _list.isEmpty();
        }

        public boolean contains(Object o) {
            return _list.contains(o);
        }

        public Iterator<T> iterator() {
            return _list.iterator();
        }

        public Object[] toArray() {
            return _list.toArray();
        }

        public <T> T[] toArray(T[] a) {
            return _list.toArray(a);
        }

        public boolean add(T e) {
            return _list.add(e);
        }

        public boolean remove(Object o) {
            return _list.remove(o);
        }

        public boolean containsAll(Collection<?> c) {
            return _list.containsAll(c);
        }

        public boolean addAll(Collection<? extends T> c) {
            return _list.addAll(c);
        }

        public boolean addAll(int index, Collection<? extends T> c) {
            return _list.addAll(index, c);
        }

        public boolean removeAll(Collection<?> c) {
            return _list.removeAll(c);
        }

        public boolean retainAll(Collection<?> c) {
            return _list.retainAll(c);
        }

        public void clear() {
            _list.clear();
        }

        public boolean equals(Object o) {
            return _list.equals(o);
        }

        public int hashCode() {
            return _list.hashCode();
        }

        public T get(int index) {
            return _list.get(index);
        }

        public T set(int index, T element) {
            return _list.set(index, element);
        }

        public void add(int index, T element) {
            _list.add(index, element);
        }

        public T remove(int index) {
            return _list.remove(index);
        }

        public int indexOf(Object o) {
            return _list.indexOf(o);
        }

        public int lastIndexOf(Object o) {
            return _list.lastIndexOf(o);
        }

        public ListIterator<T> listIterator() {
            return _list.listIterator();
        }

        public ListIterator<T> listIterator(int index) {
            return _list.listIterator(index);
        }

        public List<T> subList(int fromIndex, int toIndex) {
            return _list.subList(fromIndex, toIndex);
        }

        @Override
        public void close() throws IOException {
            // Nothing to do here.
        }
    }

    private static class InMemoryScheme<Properties, InputStream, OutputStream> extends Scheme<Properties, InputStream, OutputStream, AtomicInteger, NullContext> {

        public InMemoryScheme(Fields sourceFields, Fields sinkFields) {
            super(sourceFields, sinkFields);
        }

        @Override
        public void sourceConfInit(FlowProcess<? extends Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf) {
            // TODO anything I should be doing here?
        }

        @Override
        public void sourcePrepare(FlowProcess<? extends Properties> flowProcess, SourceCall<AtomicInteger, InputStream> sourceCall) throws IOException {
            super.sourcePrepare(flowProcess, sourceCall);
            
            sourceCall.setContext(new AtomicInteger(0));
        }
        
        @Override
        public void sinkConfInit(FlowProcess<? extends Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf) {
            // TODO anything I should be doing here?
        }

        @Override
        public boolean source(FlowProcess<? extends Properties> flowProcess, SourceCall<AtomicInteger, InputStream> sourceCall) throws IOException {
            AtomicInteger curIndex = sourceCall.getContext();
            CloseableList<TupleEntry> sourceValues = (CloseableList<TupleEntry>)sourceCall.getInput();
            if (curIndex.get() >= sourceValues.size()) {
                return false;
            } else {
                sourceCall.getIncomingEntry().setTuple(sourceValues.get(curIndex.get()).getTuple());
                curIndex.incrementAndGet();
                return true;
            }
        }

        @Override
        public void sink(FlowProcess<? extends Properties> flowProcess, SinkCall<NullContext, OutputStream> sinkCall) throws IOException {
            List<TupleEntry> list = (List<TupleEntry>)sinkCall.getOutput();
            
            // we have to clone the Tuple, so the caller can re-use it.
            list.add(new TupleEntry(getSinkFields(), new Tuple(sinkCall.getOutgoingEntry().getTuple())));
        }
    }
    
    private List<TupleEntry> _values;
    
    public InMemoryTap(Fields sourceFields) {
        this(sourceFields, sourceFields);
    }
    
    public InMemoryTap(Fields sourceFields, Fields sinkFields) {
        this(sourceFields, sinkFields, SinkMode.KEEP);
    }
    
    public InMemoryTap(Fields sourceFields, Fields sinkFields, SinkMode sinkMode) {
        super(new InMemoryScheme<Properties, InputStream, OutputStream>(sourceFields, sinkFields), sinkMode);
        
        _values = new ArrayList<TupleEntry>();
    }
    
    public List<TupleEntry> getOutput() {
        return _values;
    }
    
    @Override
    public String getIdentifier() {
        return "InMemoryTap";
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof InMemoryTap) {
            // We're only the same if we're actually the same object.
            return this == object;
        } else {
            return super.equals(object);
        }
    }
    
    @Override
    public TupleEntryIterator openForRead(FlowProcess<? extends Properties> flowProcess, InputStream input) throws IOException {
        return new TupleEntrySchemeIterator<Properties, List<TupleEntry>>(flowProcess, getScheme(), new CloseableList<TupleEntry>(_values));
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<? extends Properties> flowProcess, OutputStream output) throws IOException {
        return new TupleEntrySchemeCollector<Properties, List<TupleEntry>>(flowProcess, getScheme(), _values);
    }

    @Override
    public boolean createResource(Properties conf) throws IOException {
        // TODO clear out output list?
        _values.clear();
        return true;
    }

    @Override
    public boolean deleteResource(Properties conf) throws IOException {
        // TODO clear out output list? And StdOutTap returns false here.
        _values.clear();
        return true;
    }

    @Override
    public boolean resourceExists(Properties conf) throws IOException {
        // TODO what to return here?
        return true;
    }

    @Override
    public long getModifiedTime(Properties conf) throws IOException {
        // TODO what to return here? Mimic what StdInTap/StdOutTap do
        if (isSource()) {
            return System.currentTimeMillis();
        } else {
            return 0;
        }
    }


}
