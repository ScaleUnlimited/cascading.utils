package com.bixolabs.cascading;

import java.util.LinkedHashMap;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.operation.aggregator.First;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.AggregateBy.CompositeFunction;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("serial")
public class FirstBy extends SubAssembly {

    private static class FirstByFilter extends BaseOperation<LinkedHashMap<Tuple, Tuple>> implements Filter<LinkedHashMap<Tuple, Tuple>> {

        private Fields _groupingFields;
        private Fields _sortingFields;
        private int _threshold;
        
        public FirstByFilter(Fields groupingFields, Fields sortingFields, int threshold) {
            _groupingFields = groupingFields;
            _sortingFields = sortingFields;
            _threshold = threshold;
        }
        
        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<LinkedHashMap<Tuple, Tuple>> operationCall) {
            super.prepare(flowProcess, operationCall);

            operationCall.setContext( new LinkedHashMap<Tuple, Tuple>( _threshold, 0.75f, true ) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Tuple, Tuple> eldest) {
                    return size() > _threshold;
                }
            });
        }

        @Override
        public boolean isRemove(FlowProcess flowProcess, FilterCall<LinkedHashMap<Tuple, Tuple>> filterCall) {
            Tuple incomingGroup = filterCall.getArguments().selectTuple(_groupingFields);
            Tuple incomingSort = filterCall.getArguments().selectTuple(_sortingFields);

            Tuple curSort = filterCall.getContext().get(incomingGroup);
            if (curSort == null) {
                filterCall.getContext().put(incomingGroup, incomingSort);
                return false;
            } else if (incomingSort.compareTo(curSort) < 0) {
                filterCall.getContext().put(incomingGroup, incomingSort);
                return false;
            } else {
                return true;
            }
        }
        
    }

    public FirstBy(Pipe pipe, Fields groupingFields, Fields sortingFields) {
        this(null, pipe, groupingFields, sortingFields, Fields.ALL);
    }
    
    public FirstBy(Pipe pipe, Fields groupingFields, Fields sortingFields, boolean reverseSort) {
        this(null, pipe, groupingFields, sortingFields, reverseSort, Fields.ALL);
    }
    
    public FirstBy(String name, Pipe pipe, Fields groupingFields, Fields sortingFields, Fields declaredFields) {
        this(name, Pipe.pipes(pipe), groupingFields, sortingFields, declaredFields);
    }
    
    public FirstBy(String name, Pipe pipe, Fields groupingFields, Fields sortingFields, boolean reverseSort, Fields declaredFields) {
        this(name, Pipe.pipes(pipe), groupingFields, sortingFields, reverseSort, declaredFields, CompositeFunction.DEFAULT_THRESHOLD);
    }
    
    public FirstBy(String name, Pipe[] pipes, Fields groupingFields, Fields sortingFields, Fields declaredFields) {
        this(name, pipes, groupingFields, sortingFields, false, declaredFields, CompositeFunction.DEFAULT_THRESHOLD);
    }
    
    public FirstBy(String name, Pipe[] pipes, Fields groupingFields, Fields sortingFields, boolean reverseSort, Fields declaredFields, int threshold) {
        
        FirstByFilter filter = new FirstByFilter(groupingFields, sortingFields, threshold);
        Fields argumentSelector = Fields.merge(groupingFields, sortingFields);
        
        Pipe[] functions = new Pipe[pipes.length];
        for (int i = 0; i < functions.length; i++) {
            functions[i] = new Each(pipes[i], argumentSelector, filter);
        }
        
        Pipe pipe = new GroupBy(name, functions, groupingFields, sortingFields, reverseSort);
        First f = declaredFields.equals(Fields.ALL) ? new First() : new First(declaredFields);
        pipe = new Every(pipe, declaredFields, f, Fields.RESULTS);
        setTails( pipe );
    }

}
