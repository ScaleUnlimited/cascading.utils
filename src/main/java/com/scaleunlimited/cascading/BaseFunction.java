/**
 * Copyright 2010 TransPac Software, Inc.
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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntryCollector;

@SuppressWarnings({"serial", "rawtypes"})
public abstract class BaseFunction<INDATUM extends BaseDatum, OUTDATUM extends BaseDatum> extends BaseOperation<NullContext> implements Function<NullContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseFunction.class);
    
    private INDATUM _inDatum;
    private OUTDATUM _outDatum;
    private TupleEntryCollector _collector;
    private LoggingFlowProcess _flowProcess;
    
    public BaseFunction(Class<INDATUM> inClass, Class<OUTDATUM> outClass) throws Exception {
        super(outClass.newInstance().getFields());

        _inDatum = inClass.newInstance();
        _outDatum = outClass.newInstance();
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public final void prepare(FlowProcess process, OperationCall<NullContext> opCall) {
        super.prepare(process, opCall);
        
        _flowProcess = new LoggingFlowProcess(process);
        _collector = ((FunctionCall)opCall).getOutputCollector();
        
        try {
            prepare();
        } catch (Throwable t) {
            if (!handlePrepareException(t)) {
                LOGGER.error("Unhandled exception while preparing", t);
                if (t instanceof RuntimeException) {
                    throw (RuntimeException)t;
                } else {
                    throw new RuntimeException(t);
                }
            }
        }

    }
    
    @Override
    public final void cleanup(FlowProcess flowProcess, OperationCall<NullContext> opCall) {
        super.cleanup(flowProcess, opCall);

        try {
            cleanup();
        } catch (Throwable t) {
            if (!handleCleanupException(t)) {
                LOGGER.error("Unhandled exception while cleaning up", t);
                if (t instanceof RuntimeException) {
                    throw (RuntimeException)t;
                } else {
                    throw new RuntimeException(t);
                }
            }
        }
    }
    
    @Override
    public final void operate(FlowProcess process, FunctionCall<NullContext> funcCall) {
        _inDatum.setTupleEntry(funcCall.getArguments());
        
        try {
            process(_inDatum);
        } catch (Throwable t) {
            if (!handleProcessException(_inDatum, t)) {
                LOGGER.error("Unhandled exception while processing datum: " + safeToString(_inDatum), t);
                
                if (t instanceof RuntimeException) {
                    throw (RuntimeException)t;
                } else {
                    throw new RuntimeException(t);
                }
            }
        }
    }
    
    private String safeToString(Object o) {
        try {
            return o.toString();
        } catch (Throwable t) {
            LOGGER.error("Exception converting object to string", t);
            return "<non-stringable object>";
        }
    }
    
    public final void emit(OUTDATUM out) {
        _collector.add(out.getTuple());
    }

    public OUTDATUM getOutDatum() {
        return _outDatum;
    }
    
    public LoggingFlowProcess getFlowProcess() {
        return _flowProcess;
    }
    
    public void prepare() throws Exception {}
    abstract void process(final INDATUM in) throws Exception;
    public void cleanup() throws Exception {}
    
    public boolean handlePrepareException(Throwable t) { return false; }
    public boolean handleProcessException(final INDATUM in, Throwable t) { return false; }
    public boolean handleCleanupException(Throwable t) { return false; }

}
