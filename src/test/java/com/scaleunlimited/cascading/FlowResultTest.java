package com.scaleunlimited.cascading;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;


public class FlowResultTest {
    
    private static final long COUNTER_1_VALUE = 10L;
    private static final long COUNTER_2_VALUE = 20L;
    private static final long COUNTER_3_VALUE = 30L;
    private static final long COUNTER_4_US_VALUE = 40L;
    private static final long COUNTER_4_GB_VALUE = 50L;
    private static final long COUNTER_5_US_VALUE = 60L;
    private static final long COUNTER_5_GB_VALUE = 70L;
    
    private static final String COUNTER_5_GROUP_NAME = "counter-5";
    
    private enum TEST_COUNTERS {
        COUNTER_1,
        COUNTER_2,
        COUNTER_3,
        COUNTER_4_BY_COUNTRY,
    }
    
    FlowResult _flowResult;

    @Before
    public void setUp() throws Exception {
        Map<String, Long> counters = new HashMap<String, Long>();
        counters.put(FlowCounters.getCounterKey(TEST_COUNTERS.COUNTER_1), COUNTER_1_VALUE);
        counters.put(FlowCounters.getCounterKey(TEST_COUNTERS.COUNTER_2), COUNTER_2_VALUE);
        counters.put(FlowCounters.getCounterKey(TEST_COUNTERS.COUNTER_3), COUNTER_3_VALUE);
        counters.put(   FlowCounters.getCounterKey(TEST_COUNTERS.COUNTER_4_BY_COUNTRY.name(), "us"),
                        COUNTER_4_US_VALUE);
        counters.put(   FlowCounters.getCounterKey(TEST_COUNTERS.COUNTER_4_BY_COUNTRY.name(), "gb"),
                        COUNTER_4_GB_VALUE);
        counters.put(   FlowCounters.getCounterKey(COUNTER_5_GROUP_NAME, "us"),
                        COUNTER_5_US_VALUE);
        counters.put(   FlowCounters.getCounterKey(COUNTER_5_GROUP_NAME, "gb"),
                        COUNTER_5_GB_VALUE);
        
        _flowResult = new FlowResult(counters);
    }
    
    @Test
    public void testGetCounterValue() throws Throwable {
        assertEquals(   COUNTER_1_VALUE, 
                        _flowResult.getCounterValue(TEST_COUNTERS.COUNTER_1));
        assertEquals(   COUNTER_2_VALUE, 
                        _flowResult.getCounterValue(TEST_COUNTERS.COUNTER_2));
        assertEquals(   COUNTER_3_VALUE, 
                        _flowResult.getCounterValue(TEST_COUNTERS.COUNTER_3));
        assertEquals(   COUNTER_4_US_VALUE, 
                        _flowResult.getCounterValue(TEST_COUNTERS.COUNTER_4_BY_COUNTRY.name(), "us"));
        assertEquals(   COUNTER_4_GB_VALUE, 
                        _flowResult.getCounterValue(TEST_COUNTERS.COUNTER_4_BY_COUNTRY.name(), "gb"));
        assertEquals(   COUNTER_5_US_VALUE, 
                        _flowResult.getCounterValue(COUNTER_5_GROUP_NAME, "us"));
        assertEquals(   COUNTER_5_GB_VALUE, 
                        _flowResult.getCounterValue(COUNTER_5_GROUP_NAME, "gb"));
    }
    
    @Test
    public void testGetGroupCounterValues() throws Throwable {
        Map<String, Long> groupCounters = 
            _flowResult.getGroupCounterValues(TEST_COUNTERS.COUNTER_4_BY_COUNTRY.name());
        assertEquals(COUNTER_4_US_VALUE, (long)groupCounters.get("us"));
        assertEquals(COUNTER_4_GB_VALUE, (long)groupCounters.get("gb"));
        
        groupCounters = 
            _flowResult.getGroupCounterValues(COUNTER_5_GROUP_NAME);
        assertEquals(COUNTER_5_US_VALUE, (long)groupCounters.get("us"));
        assertEquals(COUNTER_5_GB_VALUE, (long)groupCounters.get("gb"));
    }
}
