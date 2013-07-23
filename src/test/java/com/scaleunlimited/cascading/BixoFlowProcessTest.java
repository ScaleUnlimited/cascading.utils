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

import static org.junit.Assert.*;

import org.apache.log4j.Level;
import org.junit.Test;

import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowProcess.LoggingLevels;

public class BixoFlowProcessTest {
    
    private enum TestCounter {
        COUNTER_A,
        COUNTER_B
    }
    
    @Test
    public void testLocalCounter() {
        LoggingFlowProcess process = new LoggingFlowProcess();        
        process.increment(TestCounter.COUNTER_A, 2);
        
        assertEquals(2, process.getCounter(TestCounter.COUNTER_A));
        assertEquals(0, process.getCounter(TestCounter.COUNTER_B));

        process.decrement(TestCounter.COUNTER_B, 2);
        assertEquals(-2, process.getCounter(TestCounter.COUNTER_B));
    }
    
    @Test
    public void testHadoopCounter() {
        // TODO KKr - how to test "real" Hadoop counters? Need to be running in non-local
        // mode, with a real Cascading flow.
    }
   
    // TODO VMa: fix for slf4j - see related comment where LoggingLevels is defined
    @Test
    public void testLoggingCounter() {
        assertEquals(LoggingLevels.INFO, LoggingLevels.fromLevel(Level.INFO));
        assertEquals("INFO", LoggingLevels.INFO.toString());
    }
}
