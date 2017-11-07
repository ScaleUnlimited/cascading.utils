/**
 * Copyright 2010-2013 Scale Unlimited.
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

import java.io.PrintStream;

import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.operation.Debug;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * A version of Cascading's Debug() operator with several additional features:
 * 
 * 1. You can use slf4j to log (and only if log level is <= debug).
 * 2. You can limit output length (and remove \r\n) for cleaner output.
 * 3. You can limit the maximum number of logged Tuples.
 * 4. You can log only Tuples that have a target value in one field.
 * 
 * It also has a static makePipe method, that does nothing if the log
 * level is set to > DEBUG. That way it's easy to leave code in and not
 * pay the price during production runs.
 * 
 * The four constructors that take no output parameter also use slf4j to log
 * and depend on (level > DEBUG).  The four that do take an output parameter
 * leverage Cascading's planner support and send their output directly to
 * System.err or System.out, just like Debug does.
 *
 */
@SuppressWarnings({"serial", "rawtypes"})
public class TupleLogger extends Debug {
    private static final Logger LOGGER = LoggerFactory.getLogger(TupleLogger.class);
    
    public static final int DEFAULT_MAX_ELEMENT_LENGTH = 100;
    
    // Support to explicitly and globally enable/disable tuple logging
    // (e.g., see BaseOptions.setDebugLogging).
    private static Boolean _enableTupleLogging = null;
    
    // Support (via this.setLogLevel) to explicitly control level of tuple 
    // logging when _debugOutput is null.  This can be useful in combination
    // with this.setTupleMatchFieldName/Values to log matching Tuples at WARN.
    private Level _logLevel = Level.SLF4J_DEBUG;
    
    private String _prefix = null;
    private boolean _printFields = false;
    private Output _debugOutput = null;

    private int _printFieldsEvery = 10;
    private int _printTupleEvery = 1;
    private long _printMaxTuples = Long.MAX_VALUE;
    private int _maxPrintLength = DEFAULT_MAX_ELEMENT_LENGTH;
    private String _tupleMatchFieldName = null;
    private Object[] _tupleMatchFieldValues = null;
    private Enum _tupleMatchCounter = null;
    
    private long _numMatchingTuples = 0L;
    private long _numPrintedTuples = 0L;
    
    public static Pipe makePipe(Pipe inPipe) {
        return makePipe(inPipe, inPipe.getName(), true, DEFAULT_MAX_ELEMENT_LENGTH);
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
        return makePipe(inPipe, prefix, printFields, maxLength, doTupleLogging());
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
        super();
    }

    public TupleLogger(String prefix) {
        super(prefix);
        _prefix = prefix;
    }

    public TupleLogger(boolean printFields) {
        super(printFields);
        _printFields = printFields;
    }

    public TupleLogger(String prefix, boolean printFields) {
        super(prefix, printFields);
        _prefix = prefix;
        _printFields = printFields;
    }

    public TupleLogger(Output output) {
        super(output);
        _debugOutput = output;
    }

    public TupleLogger(Output output, String prefix) {
        super(output, prefix);
        _debugOutput = output;
        _prefix = prefix;
    }

    public TupleLogger(Output output, boolean printFields) {
        super(output, printFields);
        _debugOutput = output;
        _printFields = printFields;
    }

    public TupleLogger(Output output, String prefix, boolean printFields) {
        super(output, prefix, printFields);
        _debugOutput = output;
        _prefix = prefix;
        _printFields = printFields;
    }

    /**
     * @return number of Tuples that must be logged before the next time we
     * output another header line with their field names
     */
    @Override
    public int getPrintFieldsEvery() {
        return _printFieldsEvery;
    }

    /**
     * @param printFieldsEvery number of Tuples that must be logged before
     * the next time we output another header line with their field names
     */
    @Override
    public void setPrintFieldsEvery(int printFieldsEvery) {
        _printFields = true;
        _printFieldsEvery = printFieldsEvery;
    }

    /**
     * @return number of (matching) Tuples to skip before logging the next one.
     * Note that this may be increased automatically as the total logged
     * approaches any configured maximum.
     * @see #setPrintOnlyMatchingTuples(String, Object)
     * @see #setPrintMaxTuples(long)
     */
    @Override
    public int getPrintTupleEvery() {
        return _printTupleEvery;
    }

    /**
     * @param printTupleEvery number of (matching) Tuples to skip before logging 
     * the next one.  Note that this may be increased automatically as the
     * total logged approaches any configured maximum.
     * @see #setPrintOnlyMatchingTuples(String, Object)
     * @see #setPrintMaxTuples(long)
     */
    @Override
    public void setPrintTupleEvery(int printTupleEvery) {
        _printTupleEvery = printTupleEvery;
    }

    /**
     * @return maximum number of Tuples that will be logged.
     */
    public long getPrintMaxTuples() {
        return _printMaxTuples;
    }

    /**
     * @param printMaxTuples maximum number of Tuples that will be logged.
     * Once half of this total have been logged, we automatically begin
     * skipping more and more of them.
     */
    public void setPrintMaxTuples(long printMaxTuples) {
        _printMaxTuples = printMaxTuples;
    }

    /**
     * @return maximum characters before Tuple representation is automatically
     * truncated
     */
    public int getMaxPrintLength() {
        return _maxPrintLength;
    }
    
    /**
     * @param maxPrintLength maximum characters to output before Tuple 
     * representation is automatically truncated
     */
    public void setMaxPrintLength(int maxPrintLength) {
        _maxPrintLength = maxPrintLength;
    }
    
    /**
     * Log only Tuples whose <code>fieldName</code> equals <code>targetValue</code>.
     * Note that only such matching Tuples count toward the Tuple and fields
     * header logging (i.e., it's every N <em>matching</em> Tuples, and
     * it will log a maximum of M <em>matching</em> Tuples.)
     * @param fieldName of field whose value must match
     * @param targetValue of field in matching Tuples
     */
    public void setPrintOnlyMatchingTuples(String fieldName, Object... targetValues) {
        _tupleMatchFieldName = fieldName;
        _tupleMatchFieldValues = targetValues;
    }
    
    /**
     * @param counter to increment for each Tuple matching constraints set via
     * {@link #setPrintOnlyMatchingTuples(String, Object...)}
     */
    public void setTupleMatchCounter(Enum counter) {
        _tupleMatchCounter = counter;
    }
    
    /**
     * @return Level at which TupleLogger logs (matching) Tuples whenever no
     * Output stream was provided to the constructor.
     */
    public Level getLogLevel() {
        return _logLevel;
    }

    /**
     * @param logLevel at which TupleLogger should log (matching) Tuples
     * (only valid when no Output stream was provided to the constructor).
     */
    public void setLogLevel(Level logLevel) {
        if (_debugOutput != null) {
            throw new IllegalArgumentException("Unsupported for Output: " + _debugOutput);
        }
        _logLevel = logLevel;
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall<Long> operationCall) {

        // Let Debug have its own context (which we try to keep updated)
        super.prepare(flowProcess, operationCall);
    }

    /** @see Filter#isRemove(cascading.flow.FlowProcess, FilterCall) */
    public boolean isRemove(FlowProcess flowProcess, FilterCall<Long> filterCall) {
        
        // Try to keep Debug's context updated so that its cleanup logs as
        // expected.
        Long count = filterCall.getContext();
        filterCall.setContext(count+1);
        
        if ((_debugOutput != null) || doTupleLogging()) {
            TupleEntry entry = filterCall.getArguments();
            
            // If there's a maximum # Tuples to be logged, then skip more and
            // more of them as we approach this limit.  Here we both avoid a
            // divide by zero error (as we reach the limit), and also deal with
            // the fact that Debug has limited our _printTupleEvery field to
            // an Integer.
            // TODO Should we introduce a random element?
            if (_printMaxTuples < Long.MAX_VALUE) {
                _printTupleEvery = (_numPrintedTuples < _printMaxTuples) ?
                        Math.max(   _printTupleEvery,
                                    (int)
                                    (Math.min(  (   _printMaxTuples 
                                                /   (   _printMaxTuples 
                                                    -   _numPrintedTuples)),
                                                Integer.MAX_VALUE)))
                    :   Integer.MAX_VALUE;
            }
            
            // If we're ignoring Tuples that don't have a target value in one
            // specific field, then figure out if this one matches.
            boolean isCountTuple = true;
            if (_tupleMatchFieldName != null) {
                isCountTuple = false;
                if (_tupleMatchFieldName.startsWith("*")) {
                    String tupleMatchFieldNameSuffix =
                        _tupleMatchFieldName.substring(1);
                    for (Comparable field : entry.getFields()) {
                        String fieldName = (String)field;
                        if  (   fieldName.endsWith(tupleMatchFieldNameSuffix)
                            &&  isTupleMatch(entry.getObject(fieldName))) {
                            isCountTuple = true;
                            break;
                        }
                    }
                } else {
                    isCountTuple = 
                        isTupleMatch(entry.getObject(_tupleMatchFieldName));
                }
                
                if  (   isCountTuple
                    &&  (_tupleMatchCounter != null)) {
                    flowProcess.increment(_tupleMatchCounter, 1);
                }
            }
            
            if (isCountTuple) {
                
                // Print this Tuple unless we're supposed to skip it.
                if ((_numMatchingTuples % _printTupleEvery) == 0) {

                    // If we're also printing the Field names, then do so only at
                    // regular multiples of the Tuples we've actually printed.
                    if (_printFields && ((_numPrintedTuples % _printFieldsEvery) == 0)) {
                        log(entry.getFields().print());
                    }
                    
                    StringBuilder tupleString = new StringBuilder();
                    if (_printTupleEvery > 1) {
                        String skippedNote =
                            String.format("(%,d skipped) ", _printTupleEvery - 1);
                        tupleString.append(skippedNote);
                    }
                    log(printTuple(tupleString, entry.getTuple()));
                    _numPrintedTuples++;
                }
                
                // Note that this is the count of the Tuples that made it past
                // any special field value filtering, which is not necessarily
                // the total Tuple count passing through TupleLogger.
                _numMatchingTuples++;
            }
        }
        
        // Never filter anything
        return false;
    }
    
    private boolean isTupleMatch(Object fieldValue) {
        boolean result = true;
        for (Object tupleMatchFieldValue : _tupleMatchFieldValues) {
            result = false;
            if  (   (tupleMatchFieldValue == null) ?
                    (fieldValue == null)
                :   tupleMatchFieldValue.equals(fieldValue)) {
                return true;
            }
        }
        return result;
    }
    
    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall<Long> longOperationCall) {
        
        // Delegate to Debug if we're leveraging planner support
       if (_debugOutput != null) {
            super.cleanup(flowProcess, longOperationCall);
            
        // Otherwise send the same kind of thing to slf4j
        } else if (doTupleLogging()) {
            log("tuples count: " + longOperationCall.getContext().toString());
        }
    }

    /**
     * Decide if we want to log tuples.
     * 
     * @return true if we should log tuples.
     */
    private static boolean doTupleLogging() {
        if (_enableTupleLogging != null) {
            return _enableTupleLogging;
        } else {
            return LOGGER.isDebugEnabled();
        }
    }

    private void log(String message) {
        if (_prefix != null) {
            log(new StringBuilder(message));
        } else {
            logInternal(message);
        }
    }
    
    private void log(StringBuilder message) {
        if (_prefix != null) {
            message.insert(0, ": ");
            message.insert(0, _prefix);
        }
        
        logInternal(message.toString());
    }
    
    protected void logInternal(String message) {
        if (_debugOutput == null) {
            switch (_logLevel) {
                case SLF4J_TRACE:
                    LOGGER.trace(message);
                break;
                case SLF4J_DEBUG:
                    LOGGER.debug(message);
                break;
                case SLF4J_INFO:
                    LOGGER.info(message);
                break;
                case SLF4J_WARN:
                    LOGGER.warn(message);
                break;
                case SLF4J_ERROR:
                    LOGGER.error(message);
                break;
                default:
                    throw new IllegalArgumentException(     "Unkonwn log level: " 
                                                        +   _logLevel);
            }
        } else {
            @SuppressWarnings("resource")
            PrintStream stream = 
                (   _debugOutput == Output.STDOUT ? 
                    System.out : System.err);
            stream.println(message);
        }
    }
    
    public StringBuilder printTuple(StringBuilder buffer, Tuple tuple) {
        return printTuple(buffer, tuple, _maxPrintLength);
    }
    
    public static StringBuilder printTuple( StringBuilder buffer, 
                                            Tuple tuple, 
                                            int maxPrintLength) {
        buffer.append( "[" );
        for (int i = 0; i < tuple.size(); i++) {
            Object element = tuple.getObject( i );

            if (element instanceof Tuple) {
                printTuple(buffer, (Tuple)element, maxPrintLength);
            } else {
                buffer.append("\'");
                buffer.append(printObject(element, maxPrintLength));
                buffer.append( "\'" );
            }

            if (i < tuple.size() - 1) {
                buffer.append(", ");
            }
        }
        
        buffer.append( "]" );
        return buffer;
    }
    
    @SuppressWarnings({ "deprecation" })
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
     * Explicitly set whether to enable/disable logging, which then will
     * impact what makePipe() will do.
     * 
     * @param enabled true to force logging, false to force no logging.
     */
    public static void enableLogging(boolean enabled) {
        _enableTupleLogging = enabled;
    }

}
