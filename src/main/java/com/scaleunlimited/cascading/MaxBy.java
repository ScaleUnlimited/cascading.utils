package com.scaleunlimited.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.aggregator.Max;
import cascading.pipe.assembly.AggregateBy;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;

@SuppressWarnings("serial")
public class MaxBy extends AggregateBy {
    
    @SuppressWarnings("rawtypes")
    protected static class MaxByFunctor implements AggregateBy.Functor {
        private Fields _declaredFields;
        private Class _targetFieldType;

        public MaxByFunctor(Fields targetField, Class targetFieldType) {
            if (!targetField.isDeclarator() || (targetField.size() != 1)) {
                throw new IllegalArgumentException("MaxBy can only process a single field");
            }
            
            _declaredFields = targetField;
            _targetFieldType = targetFieldType;
        }
        
        @Override
        public Fields getDeclaredFields() {
            return _declaredFields;
        }

        @Override
        public Tuple aggregate( FlowProcess flowProcess, 
                                TupleEntry args, 
                                Tuple context) {
            // We store the maximum value that we see in our target field
            if  (   (context == null)
                ||  (args.getDouble(0) > context.getDouble(0))) {
                context = new Tuple(args.get(0));
            }
            
            return context;
        }

        @Override
        public Tuple complete(FlowProcess flowProcess, Tuple context) {
            context.set(0, Tuples.coerce(context.getDouble(0), _targetFieldType));
            return context;
        }
    }
    
    public MaxBy(Fields targetField, Fields maxField, Class targetFieldType) {
        super(  targetField,
                new MaxByFunctor(maxField, targetFieldType),
                new Max(maxField, targetFieldType));
    }

}
