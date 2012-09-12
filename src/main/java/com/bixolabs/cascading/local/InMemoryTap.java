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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicInteger;

import com.bixolabs.cascading.NullContext;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.tuple.TupleEntrySchemeIterator;

@SuppressWarnings("serial")
public class InMemoryTap<Config, Input, Output> extends Tap<Config, Input, Output> {

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

    private static class InMemoryScheme<Config, Input, Output> extends Scheme<Config, Input, Output, AtomicInteger, NullContext> {

        public InMemoryScheme(Fields fields) {
            super(fields);
        }

        public InMemoryScheme(Fields sourceFields, Fields sinkFields) {
            super(sourceFields, sinkFields);
        }

        @Override
        public void sourceConfInit(FlowProcess<Config> flowProcess, Tap<Config, Input, Output> tap, Config conf) {
            // TODO anything I should be doing here?
        }

        @Override
        public void sourcePrepare(FlowProcess<Config> flowProcess, SourceCall<AtomicInteger, Input> sourceCall) throws IOException {
            super.sourcePrepare(flowProcess, sourceCall);
            
            sourceCall.setContext(new AtomicInteger(0));
        }
        
        @Override
        public void sinkConfInit(FlowProcess<Config> flowProcess, Tap<Config, Input, Output> tap, Config conf) {
            // TODO anything I should be doing here?
        }

        @Override
        public boolean source(FlowProcess<Config> flowProcess, SourceCall<AtomicInteger, Input> sourceCall) throws IOException {
            AtomicInteger curIndex = sourceCall.getContext();
            CloseableList<Tuple> sourceValues = (CloseableList<Tuple>)sourceCall.getInput();
            if (curIndex.get() >= sourceValues.size()) {
                return false;
            } else {
                sourceCall.getIncomingEntry().setTuple(sourceValues.get(curIndex.get()));
                curIndex.incrementAndGet();
                return true;
            }
        }

        @Override
        public void sink(FlowProcess<Config> flowProcess, SinkCall<NullContext, Output> sinkCall) throws IOException {
            List<TupleEntry> list = (List<TupleEntry>)sinkCall.getOutput();
            list.add(new TupleEntry(getSinkFields(), sinkCall.getOutgoingEntry().getTuple()));
        }
    }
    
    private List<Tuple> _inputValues;
    private List<TupleEntry> _outputValues;
    
    public InMemoryTap(Fields sourceFields, Tuple... inputValues) {
        super(new InMemoryScheme<Config, Input, Output>(sourceFields));
        
        _inputValues = new ArrayList<Tuple>(Arrays.asList(inputValues));
    }
    
    public InMemoryTap(Fields sinkFields) {
        super(new InMemoryScheme<Config, Input, Output>(sinkFields, sinkFields));
        
        _outputValues = new ArrayList<TupleEntry>();
    }
    
    public List<TupleEntry> getOutput() {
        return _outputValues;
    }
    
    @Override
    public String getIdentifier() {
        return "InMemoryTap";
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<Config> flowProcess, Input input) throws IOException {
        return new TupleEntrySchemeIterator<Config, List<Tuple>>(flowProcess, getScheme(), new CloseableList<Tuple>(_inputValues));
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Config> flowProcess, Output output) throws IOException {
        return new TupleEntrySchemeCollector<Config, List<TupleEntry>>(flowProcess, getScheme(), _outputValues);
    }

    @Override
    public boolean createResource(Config conf) throws IOException {
        // TODO clear out output list?
        return true;
    }

    @Override
    public boolean deleteResource(Config conf) throws IOException {
        // TODO clear out output list? And StdOutTap returns false here.
        return true;
    }

    @Override
    public boolean resourceExists(Config conf) throws IOException {
        // TODO what to return here?
        return true;
    }

    @Override
    public long getModifiedTime(Config conf) throws IOException {
        // TODO what to return here? Mimic what StdInTap/StdOutTap do
        if (isSource()) {
            return System.currentTimeMillis();
        } else {
            return 0;
        }
    }


}
