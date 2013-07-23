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

import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * A version of Cascading's Debug() operator with two key changes:
 * 
 * 1. Use Log4J to log (and only if log level is <= debug)
 * 2. Limit output length (and remove \r\n) for cleaner output.
 * 
 * It also has a static makePipe method, that does nothing if the log
 * level is set to > DEBUG. That way it's easy to leave code in and not
 * pay the price during production runs.
 *
 */
@SuppressWarnings("serial")
public class TupleLogger extends BaseOperation<Long> implements Filter<Long> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TupleLogger.class);
    
    public static final int DEFAULT_MAX_ELEMENT_LENGTH = 100;
    
    private String _prefix = null;
    private boolean _printFields = false;

    private int _printFieldsEvery = 10;
    private int _printTupleEvery = 1;
    private int _maxPrintLength = DEFAULT_MAX_ELEMENT_LENGTH;
    
    public static Pipe makePipe(Pipe inPipe) {
        return makePipe(inPipe, inPipe.getName(), true, DEFAULT_MAX_ELEMENT_LENGTH, LOGGER.isDebugEnabled());
    }
    
    public static Pipe makePipe(Pipe inPipe, boolean printFields) {
        return makePipe(inPipe, printFields, DEFAULT_MAX_ELEMENT_LENGTH);
    }
    
    public static Pipe makePipe(Pipe inPipe, boolean printFields, boolean makePipe) {
        return makePipe(inPipe, inPipe.getName(), printFields, DEFAULT_MAX_ELEMENT_LENGTH, makePipe);
    }
    
    public static Pipe makePipe(Pipe inPipe, String prefix, boolean printFields) {
        return makePipe(inPipe, prefix, printFields, DEFAULT_MAX_ELEMENT_LENGTH);
    }
    
    /**
     * Uses inPipe.getName() for the logging prefix. Convenient, but may not be
     * appropriate for pipes with long names.
     * 
     * @param inPipe input pipe
     * @param printFields print field names
     * @param maxLength max length of any element string.
     * @return pipe to use
     */
    public static Pipe makePipe(Pipe inPipe, boolean printFields, int maxLength) {
        return makePipe(inPipe, inPipe.getName(), printFields, maxLength);
    }
    
    /**
     * Don't create a new operation if nothing will get logged.
     * 
     * Note this precludes dynamically changing the logging level, as the Flow will
     * be created without the TupleLogger if the debug level is set to > DEBUG when
     * the Flow is being created.
     * 
     * @param inPipe input pipe
     * @param prefix prefix to use when printing results
     * @param printFields print field names
     * @param maxLength max length of any element string.
     * @return pipe to use
     */
    public static Pipe makePipe(Pipe inPipe, String prefix, boolean printFields, int maxLength) {
        return makePipe(inPipe, prefix, printFields, maxLength, LOGGER.isDebugEnabled());
    }
    
    public static Pipe makePipe(Pipe inPipe, String prefix, boolean printFields, int maxLength, boolean addPipe) {
        if (addPipe) {
            TupleLogger tl = new TupleLogger(prefix, printFields);
            tl.setMaxPrintLength(maxLength);
            
            return new Each(inPipe, tl);
        } else {
            return inPipe;
        }
    }
    
    public TupleLogger() {
    }

    public TupleLogger(String prefix) {
        _prefix = prefix;
    }

    public TupleLogger(String prefix, boolean printFields) {
        _prefix = prefix;
        _printFields = printFields;
    }

    public TupleLogger(boolean printFields) {
        _printFields = printFields;
    }

    public int getPrintFieldsEvery() {
        return _printFieldsEvery;
    }

    public void setPrintFieldsEvery( int printFieldsEvery ) {
        _printFieldsEvery = printFieldsEvery;
    }

    public int getPrintTupleEvery() {
        return _printTupleEvery;
    }

    public void setPrintTupleEvery( int printTupleEvery ) {
        _printTupleEvery = printTupleEvery;
    }

    public int getMaxPrintLength() {
        return _maxPrintLength;
    }
    
    public void setMaxPrintLength(int maxPrintLength) {
        _maxPrintLength = maxPrintLength;
    }
    
    @Override
    public void prepare( FlowProcess flowProcess, OperationCall<Long> operationCall ) {
        super.prepare(flowProcess, operationCall);

        operationCall.setContext(0L);
    }

    /** @see Filter#isRemove(cascading.flow.FlowProcess, FilterCall) */
    public boolean isRemove( FlowProcess flowProcess, FilterCall<Long> filterCall ) {
        if (LOGGER.isDebugEnabled()) {
            long count = filterCall.getContext();
            TupleEntry entry = filterCall.getArguments();
            
            if (_printFields && ((count % _printFieldsEvery) == 0)) {
                log(entry.getFields().print());
            }
            
            if ((count % _printTupleEvery) == 0) {
                StringBuilder tupleString = new StringBuilder();
                log(printTuple(tupleString, entry.getTuple()));
            }
            
            filterCall.setContext(count + 1);
        }
        
        // Never filter anything
        return false;
    }

    private void log(String message) {
        if (_prefix != null) {
            log(new StringBuilder(message));
        } else {
            LOGGER.debug(message);
        }
    }
    
    private void log(StringBuilder message) {
        if (_prefix != null) {
            message.insert(0, ": ");
            message.insert(0, _prefix);
        }
        
        LOGGER.debug(message.toString());
    }
    
    @SuppressWarnings("unchecked")
    public StringBuilder printTuple(StringBuilder buffer, Tuple tuple) {
        buffer.append( "[" );
        for (int i = 0; i < tuple.size(); i++) {
            Object element = tuple.getObject( i );

            if (element instanceof Tuple) {
                printTuple(buffer, (Tuple)element);
            } else {
                buffer.append("\'");
                buffer.append(printObject(element, _maxPrintLength));
                buffer.append( "\'" );
            }

            if (i < tuple.size() - 1) {
                buffer.append(", ");
            }
        }
        
        buffer.append( "]" );
        return buffer;
    }
    
    @SuppressWarnings({ "unchecked", "deprecation" })
    public static String printObject(Object element, int maxLength) {
        StringBuilder result = new StringBuilder();
        
        if (element == null) {
            result.append("null");
        } else if (element instanceof String) {
            result.append((String)element);
        } else if (element instanceof BytesWritable) {
            // Use get() vs. getBytes() so we work with Hadoop 0.18.3
            byte[] bytes = ((BytesWritable)element).get();
            int numBytes = Math.min(bytes.length, maxLength/3);
            
            for (int i = 0; i < numBytes; i++) {
                result.append(String.format("%02X", bytes[i]));
                result.append(' ');
            }
            
            // Get rid of extra trailing space.
            if (numBytes > 0) {
                return result.substring(0, result.length() - 1);
            }
        } else {
            result.append(element.toString());
        }
        
        // Now we want to limit the length, and get rid of control, \n, \r sequences
        return result.substring(0, Math.min(maxLength, result.length())).replaceAll("[\r\n\t]", " ");
    }
    
    /**
     * Explicitly set the logging level for our internal logger, which then will
     * impact what makePipe() will do.
     * 
     * @param level new logging level
     */
    public static void setLevel(Level level) {
        // TODO VMa: fix for slf4j
        LOGGER.setLevel(level);
    }

}
