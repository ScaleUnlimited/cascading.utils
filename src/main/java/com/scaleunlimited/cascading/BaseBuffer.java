package com.scaleunlimited.cascading;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings("serial")
public abstract class BaseBuffer extends BaseOperation<NullContext> implements Buffer<NullContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseBuffer.class);
    
    @SuppressWarnings("rawtypes")
    protected transient LoggingFlowProcess _flowProcess;
    protected transient TupleEntryCollector _collector;

    public BaseBuffer(Fields resultFields) {
        super(resultFields);
    }
    
    // Classes extending BaseBuffer must implement this method
    abstract public void process(BufferCall<NullContext> bufferCall) throws Exception;

    // Default, do-nothing implementations of overridable methods
    public void prepare() throws Exception {}
    public void cleanup() throws Exception {}
    
    public boolean handlePrepareException(Throwable t) { return false; }
    public boolean handleCleanupException(Throwable t) { return false; }
    

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
        super.prepare(flowProcess, operationCall);
        
        _flowProcess = new LoggingFlowProcess(flowProcess);
        _flowProcess.addReporter(new LoggingFlowReporter());
        
        String bufferClassName = this.getClass().getSimpleName();
        
        try {
            LOGGER.info("Starting " + bufferClassName);
            
            prepare();
        } catch (Throwable t) {
            if (!handlePrepareException(t)) {
                LOGGER.error("Unhandled exception while preparing " + bufferClassName, t);
                if (t instanceof RuntimeException) {
                    throw (RuntimeException)t;
                } else {
                    throw new RuntimeException(t);
                }
            }
        }
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
        String bufferClassName = this.getClass().getSimpleName();

        try {
            LOGGER.info("Ending " + bufferClassName);

            cleanup();
        } catch (Throwable t) {
            if (!handleCleanupException(t)) {
                LOGGER.error("Unhandled exception while cleaning up " + bufferClassName, t);
                if (t instanceof RuntimeException) {
                    throw (RuntimeException)t;
                } else {
                    throw new RuntimeException(t);
                }
            }
        }

        _flowProcess.dumpCounters();
        
        super.cleanup(flowProcess, operationCall);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall<NullContext> bufferCall) {
        if (_collector == null) {
            _collector = bufferCall.getOutputCollector();
        }
        
        try {
            process(bufferCall);
        } catch (Throwable t) {
            LOGGER.error("Unhandled exception while processing group: " + safeToString(bufferCall.getGroup()), t);

            if (t instanceof RuntimeException) {
                throw (RuntimeException)t;
            } else {
                throw new RuntimeException(t);
            }
        }
    }
    
    protected void emit(TupleEntry out) {
        _collector.add(out);
    }

    protected void emit(Tuple out) {
        _collector.add(out);
    }

    @SuppressWarnings("rawtypes")
    protected void incrementCounter(Enum counter, long amount) {
        // Work around Cascading API bug where it only takes an int, not a long.
        while (amount > Integer.MAX_VALUE) {
            _flowProcess.increment(counter, Integer.MAX_VALUE);
            amount -= Integer.MAX_VALUE;
        }
            
        _flowProcess.increment(counter, (int)amount);
    }
    
    private String safeToString(TupleEntry te) {
        try {
            return te.toString();
        } catch (Throwable t) {
            LOGGER.error("Exception converting TupleEntry to string", t);
            return "<non-stringable object>";
        }
    }
    

}
