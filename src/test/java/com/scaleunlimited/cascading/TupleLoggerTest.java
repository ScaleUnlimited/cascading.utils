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
import org.junit.Test;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class TupleLoggerTest extends CascadingTestCase {

    private static final Fields TEST_FIELDS = new Fields("index", "matchString");

    @Test
    public void testLimitStringLength() {
        assertEquals("abc", TupleLogger.printObject("abcdefg", 3));
    }
    
    @Test
    public void testLimitBytesWritable() {
        BytesWritable bw = new BytesWritable("0123".getBytes());
        
        assertEquals("30 31 32", TupleLogger.printObject(bw, 10));
    }
    
    @Test
    public void testRemovingCRLF() {
        assertEquals("ab cd", TupleLogger.printObject("ab\rcd", 10));
    }
    
    @Test
    public void testEmptyBytesWritable() {
        assertEquals("", TupleLogger.printObject(new BytesWritable(), 10));
    }
    
    @Test
    public void testPrintingTupleInTuple() {
        TupleLogger tl = new TupleLogger(true);
        tl.setMaxPrintLength(10);
        
        BytesWritable bw = new BytesWritable("0123456789".getBytes());

        Tuple tuple = new Tuple("A long string", 1000, bw, new Tuple("a", "b"));
        StringBuilder result = new StringBuilder();
        result = tl.printTuple(result, tuple);
        
        assertEquals("['A long str', '1000', '30 31 32', ['a', 'b']]", result.toString());
    }
    
    @Test
    public void testPrintingNull() {
        TupleLogger tl = new TupleLogger(true);
        Tuple tuple = new Tuple("a", null);
        StringBuilder result = new StringBuilder();
        result = tl.printTuple(result, tuple);
        
        assertEquals("['a', 'null']", result.toString());
    }
    
    public static class CountingTupleLogger extends TupleLogger {
        private long _numTuplesLogged = 0;
        private long _numFieldLinesLogged = 0;
        
        public CountingTupleLogger() {
            super();
        }

        public CountingTupleLogger(boolean printFields) {
            super(printFields);
        }

        public CountingTupleLogger(String prefix, boolean printFields) {
            super(prefix, printFields);
        }

        public CountingTupleLogger(String prefix) {
            super(prefix);
        }

        @Override
        protected void logInternal(String message) {
            super.logInternal(message);
            if (message.contains("matchString")) {
                _numFieldLinesLogged++;
            } else if (!(message.contains("tuples count"))) {
                _numTuplesLogged++;
            }
        }

        public long getNumTuplesLogged() {
            return _numTuplesLogged;
        }

        public long getNumFieldLinesLogged() {
            return _numFieldLinesLogged;
        }
    }
    
    @Test
    public void testMaxTuples() {
        int numArguments = 1000 * 100;
        TupleEntry[] argumentsArray = new TupleEntry[numArguments];
        for (int i = 0; i < numArguments; i++) {
            argumentsArray[i] = makeArguments(i, i % 100);
        }
        CountingTupleLogger tupleLogger = new CountingTupleLogger(true);
        tupleLogger.setPrintMaxTuples(200);
        tupleLogger.setPrintTupleEvery(2);
        tupleLogger.setPrintFieldsEvery(10);
        tupleLogger.setPrintOnlyMatchingTuples("matchString", "match-77");
        invokeFilter(tupleLogger, argumentsArray);
        assertEquals(200, tupleLogger.getNumTuplesLogged());
    }
    
    @Test
    public void testMatchingTuples() {
        int numArguments = 10 * 100;
        TupleEntry[] argumentsArray = new TupleEntry[numArguments];
        for (int i = 0; i < numArguments; i++) {
            argumentsArray[i] = makeArguments(i, i % 100);
        }
        CountingTupleLogger tupleLogger = new CountingTupleLogger(true);
        tupleLogger.setPrintOnlyMatchingTuples("matchString", "match-3", "match-77", "match-89");
        tupleLogger.setLogLevel(Level.SLF4J_WARN);
        invokeFilter(tupleLogger, argumentsArray);
        assertEquals(30, tupleLogger.getNumTuplesLogged());

        tupleLogger = new CountingTupleLogger(true);
        tupleLogger.setPrintOnlyMatchingTuples("*String", "match-3", "match-77", "match-89");
        invokeFilter(tupleLogger, argumentsArray);
        assertEquals(30, tupleLogger.getNumTuplesLogged());

        tupleLogger = new CountingTupleLogger(true);
        tupleLogger.setPrintOnlyMatchingTuples("*Stringy", "match-3", "match-77", "match-89");
        invokeFilter(tupleLogger, argumentsArray);
        assertEquals(0, tupleLogger.getNumTuplesLogged());
    }
    
    private static TupleEntry makeArguments(long tupleIndex, long matchIndex) {
        return new TupleEntry(  TEST_FIELDS,
                                new Tuple(tupleIndex, "match-" + matchIndex));
    }
                    
}
