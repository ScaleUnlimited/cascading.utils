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
    
    @SuppressWarnings("unchecked")
    public long getCounterValue(Enum counter) {
        String counterName = counter.getClass().getName() + "." + counter.name();
        Long result = _counters.get(counterName);
        return (result == null) ? 0 : result;
    }
}
