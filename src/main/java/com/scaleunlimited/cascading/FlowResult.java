package com.scaleunlimited.cascading;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class FlowResult {
    
    private Map<String, Long> _counters;
    
    public FlowResult(Map<String, Long> counters) {
        _counters = counters;
    }
    
    public Map<String, Long> getCounters() {
        return _counters;
    }
    
    @SuppressWarnings("rawtypes")
    public long getCounterValue(Enum counter) {
        Long result = _counters.get(FlowCounters.getCounterKey(counter));
        return (result == null) ? 0 : result;
    }
    
    public long getCounterValue(String group, String counter) {
        Long result = _counters.get(FlowCounters.getCounterKey(group, counter));
        return (result == null) ? 0 : result;
    }
    
    public Map<String, Long> getGroupCounterValues(String group) {
        Map<String, Long>result = new HashMap<String, Long>();
        String groupCounterKeyPrefix = FlowCounters.getCounterKey(group, "");
        int prefixLength = groupCounterKeyPrefix.length();
        for (Entry<String, Long> entry : _counters.entrySet()) {
            String counterKey = entry.getKey();
            if (counterKey.startsWith(groupCounterKeyPrefix)) {
                String counterKeySuffix = counterKey.substring(prefixLength);
                result.put(counterKeySuffix, entry.getValue());
            }
        }
        return result;
    }
}
