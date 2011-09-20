/**
 * Copyright 2011 TransPac Software, Inc.
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

package com.bixolabs.cascading;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import cascading.flow.Flow;
import cascading.stats.FlowStats;
import cascading.stats.StepStats;

public class FlowCounters {
    static final Logger LOGGER = Logger.getLogger(FlowCounters.class);

    /**
     * Run the flow, and return back a Map that has entries for every requested
     * counter. Note that a Flow with multiple steps (Hadoop jobs) will sum the
     * counter values for all jobs, but warn when that happens.
     * 
     * @param flow Flow to be run & counted
     * @param counters Which counters to return in the map.
     * @return Map of counter enum to counts.
     */
    @SuppressWarnings("unchecked")
    public static Map<Enum, Long> run(Flow flow, Enum... counters) {

        flow.complete();
        
        Map<Enum, Long> result = new HashMap<Enum, Long>();
        
        FlowStats stats = flow.getFlowStats();
        List<StepStats> stepStats = stats.getStepStats();

        for (StepStats stepStat : stepStats) {
            for (Enum counter : counters) {
                long counterValue = stepStat.getCounterValue(counter);
                if (counterValue != 0) {
                    if (result.containsKey(counter)) {
                        LOGGER.warn("Multiple steps in flow are returning the same counter: " + counter);
                        counterValue += result.get(counter);
                    }

                    result.put(counter, counterValue);
                }
            }
        }
        
        // Make sure every enum is represented, so callers don't have to check for nulls.
        for (Enum counter : counters) {
            if (result.get(counter) == null) {
                result.put(counter, 0L);
            }
        }
        
        return result;
    }
    
    // TODO Use this routine with the above code? Would need to map from Enum to name,
    // compare against what we get back here.
    public Map<String, Long> getCounters(Flow flow) {
        Map<String, Long> result = new HashMap<String, Long>();
        
        FlowStats stats = flow.getFlowStats();
        List<StepStats> stepStats = stats.getStepStats();

        for (StepStats stepStat : stepStats) {
            Collection<String> counterGroups = stepStat.getCounterGroups();
            for (String counterGroup : counterGroups) {
                Collection<String> counters = stepStat.getCountersFor(counterGroup);
                for (String counter : counters) {
                    long counterValue = stepStat.getCounterValue(counterGroup, counter);
                    result.put(counterGroup + "." + counter, counterValue);
                }
            }
        }

        return result;
    }
    
}
