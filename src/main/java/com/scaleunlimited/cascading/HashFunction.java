package com.scaleunlimited.cascading;

import java.io.Serializable;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("serial")
public class HashFunction<V extends Serializable> extends BaseOperation<Void> implements Function<Void> {

    private Map<String, V> map;
    
    private transient Tuple result;
    
    // TODO force map to be serializable
    public HashFunction(Map<String, V> map, Fields valueField) {
        super(1, valueField);
        
        // TODO verify valueField is one field, not special.
        
        this.map = map;
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Void> operationCall) {
        super.prepare(flowProcess, operationCall);
        
        result = Tuple.size(1);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall) {
        String key = functionCall.getArguments().getString(0);
        V value = this.map.get(key);
        if (value != null) {
            result.set(0, value);
            functionCall.getOutputCollector().add(result);
        }
    }
}
