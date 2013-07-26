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

import static org.junit.Assert.*;

import org.junit.Test;

import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowProcess.LoggingLevels;

public class LoggingFlowProcessTest {
    
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
   
    @Test
    public void testLoggingCounter() {
        LoggingFlowProcess process = new LoggingFlowProcess();        
        process.setStatus(Level.SLF4J_INFO, "some msg");
        
        assertEquals(1, process.getCounter(LoggingFlowProcess.LoggingLevels.INFO));
    }
}
