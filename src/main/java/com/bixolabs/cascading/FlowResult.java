package com.bixolabs.cascading;

import java.util.Map;

public class FlowResult {
    
    private Map<String, Long> _counters;
    
    public FlowResult(Map<String, Long> counters) {
        _counters = counters;
    }
    
    public Map<String, Long> getCounters() {
        return _counters;
    }
}
