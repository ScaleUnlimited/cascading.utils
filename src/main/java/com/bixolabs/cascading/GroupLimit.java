package com.bixolabs.cascading;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class GroupLimit extends BaseOperation<NullContext> implements Buffer<NullContext> {

    private int _maxInGroup;
    
    public GroupLimit(int maxInGroup) {
        super(Fields.ARGS);
        
        if (maxInGroup < 1) {
            throw new IllegalArgumentException("maxInGroup parameter must be > 0");
        }
        
        _maxInGroup = maxInGroup;
    }
    
    @Override
    public void operate(FlowProcess flowProcess, BufferCall<NullContext> bufferCall) {
        
        int curCount = 0;
        Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
        while (iter.hasNext() && (curCount < _maxInGroup)) {
            bufferCall.getOutputCollector().add(iter.next());
            curCount += 1;
        }
    }

}
