package com.scaleunlimited.cascading;

import java.beans.ConstructorProperties;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Unique;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class UniqueCount extends SubAssembly {

    /**
     * Class FilterPartialDuplicates is a {@link cascading.operation.Filter}
     * that is used to remove observed duplicates from the tuple stream.
     * <p/>
     * Use this class typically in tandem with a
     * {@link cascading.operation.aggregator.First}
     * {@link cascading.operation.Aggregator} in order to improve de-duping
     * performance by removing as many values as possible before the
     * intermediate {@link cascading.pipe.GroupBy} operator.
     * <p/>
     * The {@code threshold} value is used to maintain a LRU of a constant size.
     * If more than threshold unique values are seen, the oldest cached values
     * will be removed from the cache.
     * 
     * @see Unique
     */
    public static class FilterPartialDuplicates extends BaseOperation<LinkedHashMap<Tuple, Object>> implements Filter<LinkedHashMap<Tuple, Object>> {
        private int threshold = 10000;

        /**
         * Constructor FilterPartialDuplicates creates a new
         * FilterPartialDuplicates instance.
         */
        public FilterPartialDuplicates() {
        }

        /**
         * Constructor FilterPartialDuplicates creates a new
         * FilterPartialDuplicates instance.
         * 
         * @param threshold
         *            of type int
         */
        @ConstructorProperties({ "threshold" })
        public FilterPartialDuplicates(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<LinkedHashMap<Tuple, Object>> operationCall) {
            operationCall.setContext(new LinkedHashMap<Tuple, Object>(threshold, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry eldest) {
                    return size() > threshold;
                }
            });
        }

        @Override
        public boolean isRemove(FlowProcess flowProcess, FilterCall<LinkedHashMap<Tuple, Object>> filterCall) {
            // we assume its more painful to create lots of tuple copies vs
            // comparisons
            Tuple args = filterCall.getArguments().getTuple();

            if (filterCall.getContext().containsKey(args))
                return true;

            filterCall.getContext().put(filterCall.getArguments().getTupleCopy(), null);

            return false;
        }

        @Override
        public void cleanup(FlowProcess flowProcess, OperationCall<LinkedHashMap<Tuple, Object>> operationCall) {
            operationCall.setContext(null);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object)
                return true;
            if (!(object instanceof FilterPartialDuplicates))
                return false;
            if (!super.equals(object))
                return false;

            FilterPartialDuplicates that = (FilterPartialDuplicates) object;

            if (threshold != that.threshold)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + threshold;
            return result;
        }
    }

    private static class CountUniques extends BaseOperation<NullContext> implements Buffer<NullContext> {

        private Fields _uniqueFields;

        private transient Tuple _result;;

        public CountUniques(Fields uniqueFields, Fields countField) {
            super(countField);

            _uniqueFields = uniqueFields;
        }

        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);

            _result = new Tuple(0);
        }

        @Override
        public void operate(FlowProcess flowProcess, BufferCall<NullContext> bufferCall) {
            // We are being called with all entries for the target group, sorted
            // by _uniqueFields
            // Keep track of the current value(s) for the unique fields, and
            // when it changes
            // increment the count. At the end we can emit the result.

            Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
            int count = 0;

            Tuple oldGroupValue = null;
            while (iter.hasNext()) {
                Tuple curGroupValue = iter.next().selectTuple(_uniqueFields);
                if (oldGroupValue == null) {
                    oldGroupValue = curGroupValue;
                }

                if (!oldGroupValue.equals(curGroupValue)) {
                    count += 1;
                    oldGroupValue = curGroupValue;
                }
            }

            _result.set(0, count);
            bufferCall.getOutputCollector().add(_result);
        }
    }

    /**
     * Constructor Unique creates a new Unique instance.
     * 
     * @param pipe
     *            of type Pipe
     * @param uniqueFields
     *            of type Fields
     */
    @ConstructorProperties({ "pipe", "uniqueFields" })
    public UniqueCount(Pipe pipe, Fields groupFields, Fields uniqueFields, Fields countField) {
        this(null, pipe, groupFields, uniqueFields, countField);
    }

    /**
     * Constructor Unique creates a new Unique instance.
     * 
     * @param pipe
     *            of type Pipe
     * @param uniqueFields
     *            of type Fields
     * @param threshold
     *            of type int
     */
    @ConstructorProperties({ "pipe", "uniqueFields", "threshold" })
    public UniqueCount(Pipe pipe, Fields groupFields, Fields uniqueFields, Fields countField, int threshold) {
        this(null, pipe, groupFields, uniqueFields, countField, threshold);
    }

    /**
     * Constructor UniqueCount creates a new UniqueCount instance.
     * 
     * @param name
     *            of type String
     * @param pipe
     *            of type Pipe
     * @param uniqueFields
     *            of type Fields
     */
    @ConstructorProperties({ "name", "pipe", "uniqueFields" })
    public UniqueCount(String name, Pipe pipe, Fields groupFields, Fields uniqueFields, Fields countField) {
        this(name, pipe, groupFields, uniqueFields, countField, 10000);
    }

    /**
     * Constructor UniqueCount creates a new UniqueCount instance.
     * 
     * @param name
     *            of type String
     * @param pipe
     *            of type Pipe
     * @param uniqueFields
     *            of type Fields
     * @param threshold
     *            of type int
     */
    @ConstructorProperties({ "name", "pipe", "uniqueFields", "threshold" })
    public UniqueCount(String name, Pipe pipe, Fields groupFields, Fields uniqueFields, Fields countField, int threshold) {
        this(name, Pipe.pipes(pipe), groupFields, uniqueFields, countField, threshold);
    }

    /**
     * Constructor UniqueCount creates a new UniqueCount instance.
     * 
     * @param pipes
     *            of type Pipe[]
     * @param uniqueFields
     *            of type Fields
     */
    @ConstructorProperties({ "pipes", "uniqueFields" })
    public UniqueCount(Pipe[] pipes, Fields groupFields, Fields uniqueFields, Fields countField) {
        this(null, pipes, groupFields, uniqueFields, countField, 10000);
    }

    /**
     * Constructor UniqueCount creates a new UniqueCount instance.
     * 
     * @param pipes
     *            of type Pipe[]
     * @param uniqueFields
     *            of type Fields
     * @param threshold
     *            of type int
     */
    @ConstructorProperties({ "pipes", "uniqueFields", "threshold" })
    public UniqueCount(Pipe[] pipes, Fields groupFields, Fields uniqueFields, Fields countField, int threshold) {
        this(null, pipes, groupFields, uniqueFields, countField, threshold);
    }

    /**
     * Constructor UniqueCount creates a new UniqueCount instance.
     * 
     * @param name
     *            of type String
     * @param pipes
     *            of type Pipe[]
     * @param uniqueFields
     *            of type Fields
     */
    @ConstructorProperties({ "name", "pipes", "uniqueFields" })
    public UniqueCount(String name, Pipe[] pipes, Fields groupFields, Fields uniqueFields, Fields countField) {
        this(name, pipes, groupFields, uniqueFields, countField, 10000);
    }

    /**
     * Constructor UniqueCount creates a new UniqueCount instance. This will
     * count the number of unique values found in uniqueFields, for each group
     * defined by groupFields, and put the resulting count into countField.
     * 
     * @param name
     *            of type String
     * @param pipes
     *            of type Pipe[]
     * @param uniqueFields
     *            of type Fields
     * @param threshold
     *            of type int
     */
    @ConstructorProperties({ "name", "pipes", "uniqueFields", "threshold" })
    public UniqueCount(String name, Pipe[] pipes, Fields groupFields, Fields uniqueFields, Fields countField, int threshold) {
        Fields joinedFields = Fields.join(groupFields, uniqueFields);
        
        Pipe[] filters = new Pipe[pipes.length];
        FilterPartialDuplicates partialDuplicates = new FilterPartialDuplicates(threshold);

        for (int i = 0; i < filters.length; i++) {
            filters[i] = new Each(pipes[i], joinedFields, partialDuplicates);
        }
        
        // At this point we need to group by the groupFields, sort by
        // uniqueFields, and then use a special CountUnique
        // buffer that uses the sort order to generate unique counts.
        
        // The output should be a tuple with the groupFields and the countField
        Pipe pipe = new GroupBy(name, filters, groupFields, uniqueFields);
        pipe = new Every(pipe, uniqueFields, new CountUniques(uniqueFields, countField), Fields.REPLACE);

        setTails(pipe);
    }
}
