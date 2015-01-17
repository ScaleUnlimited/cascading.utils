/**
 * Copyright 2011-2014 Scale Unlimited.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.Flow;
import cascading.stats.FlowStats;
import cascading.stats.FlowStepStats;

@SuppressWarnings("rawtypes")
public class FlowCounters {
    static final Logger LOGGER = LoggerFactory.getLogger(FlowCounters.class);

    /**
     * Run the flow, and return back a Map that has entries for every requested
     * counter *group* where that counter has been set during the Flow execution.
     * Note that a Flow with multiple steps (Hadoop jobs) will sum the counter
     * values for all jobs, but warn when that happens.
     * 
     * Note we have to have the funky counterGroup and extraGroups to avoid
     * a collision with the existing run(flow) method, if only a Flow is passed.
     * 
     * @param flow Flow to be run & counted
     * @param counterGroup Counter group to return.
     * @param extraGroups Additional counter groups to return.
     * @return Map of counter enum => counts.
     */
    public static Map<Enum, Long> run(Flow flow, Class<? extends Enum> counterGroup, Class<? extends Enum>... extraGroups) {
        
        Map<Enum, Long> result = new HashMap<Enum, Long>();
        
        flow.complete();

        List<Class<? extends Enum>> enums = new ArrayList<Class<? extends Enum>>(Arrays.asList(extraGroups));
        enums.add(counterGroup);
        
        FlowStats stats = flow.getFlowStats();
        List<FlowStepStats> stepStats = stats.getFlowStepStats();
        for (FlowStepStats stepStat : stepStats) {
            for (Class<? extends Enum> group : enums) {
                for (Enum counter : group.getEnumConstants()) {
                    long counterValue = stepStat.getCounterValue(counter);
                    if (counterValue != 0) {
                        if (result.containsKey(counter)) {
                            counterValue += result.get(counter);
                        }

                        result.put(counter, counterValue);
                    }
                }
            }
        }
        
        return result;
    }
    

    /**
     * Run the flow, and return back a Map that has entries for every requested
     * counter. Note that a Flow with multiple steps (Hadoop jobs) will sum the
     * counter values for all jobs, but warn when that happens.
     * 
     * If no counters are passed, we'll return all available counters that are
     * defined using Enums. This means you won't get a counter back if it was
     * logged using a group name/counter name instead of an Enum.
     * 
     * @param flow Flow to be run & counted
     * @param counters Which counters to return in the map.
     * @return Map of counter enum to counts.
     */
    public static Map<Enum, Long> run(Flow flow, Enum... counters) {

        flow.complete();
        
        Map<Enum, Long> result = new HashMap<Enum, Long>();
        
        FlowStats stats = flow.getFlowStats();
        List<FlowStepStats> stepStats = stats.getFlowStepStats();

        for (FlowStepStats stepStat : stepStats) {
            if (counters.length == 0) {
                // We want all counters.
                for (String groupName : stepStat.getCounterGroups()) {
                    Class<? extends Enum> groupClass;
                    try {
                        groupClass = (Class<? extends Enum>)FlowCounters.class.forName(groupName);
                    } catch (ClassNotFoundException e) {
                        // Probably a counter defined using strings vs. Enum, so skip it
                        continue;
                    }
                    
                    for (String counterName : stepStat.getCountersFor(groupClass)) {
                        Enum counterEnum = Enum.valueOf(groupClass, counterName);
                        long counterValue = stepStat.getCounterValue(counterEnum);
                        if (counterValue != 0) {
                            if (result.containsKey(counterEnum)) {
                                counterValue += result.get(counterEnum);
                            }

                            result.put(counterEnum, counterValue);
                        }
                    }
                }
            } else {
                for (Enum counter : counters) {
                    long counterValue = stepStat.getCounterValue(counter);
                    if (counterValue != 0) {
                        if (result.containsKey(counter)) {
                            counterValue += result.get(counter);
                        }

                        result.put(counter, counterValue);
                    }
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
    
    /**
     * Run the flow, and return back a Map that has String-keyed entries for every counter.
     * Note that a Flow with multiple steps (Hadoop jobs) will return back the sum of counter
     * values for all jobs.
     * 
     * The map's keys are <group name>.<counter name>
     * 
     * @param flow Flow to be run & counted
     * @return Map of counter name to counts.
     */
    public static Map<String, Long> runAndReturnAllCounters(Flow flow) {

        flow.complete();
        
        Map<String, Long> result = new HashMap<String, Long>();
        
        FlowStats stats = flow.getFlowStats();
        List<FlowStepStats> stepStats = stats.getFlowStepStats();

        for (FlowStepStats stepStat : stepStats) {
            for (String counterGroup : stepStat.getCounterGroups()) {
                Collection<String> counterNames = stepStat.getCountersFor(counterGroup);

                for (String counterName : counterNames) {
                    String keyName = String.format("%s.%s", counterGroup, counterName);
                    long counterValue = stepStat.getCounterValue(counterGroup, counterName);
                    if (counterValue != 0) {
                        if (result.containsKey(keyName)) {
                            counterValue += result.get(keyName);
                        }

                        result.put(keyName, counterValue);
                    }
                }
            }
        }
        
        return result;
    }
    
    // This is how LocalStepStats.increment(Enum) and LocalStepStats.getCounterValue(Enum)
    // are currently implemented and seems to match the Hadoop internal implementation as well.
    public static String getCounterKey(Enum counter) {
        return getCounterKey(counter.getDeclaringClass().getName(), counter.name());
    }
    
    // This is how we store grouped counters in the map returned by getCounters.
    public static String getCounterKey(String groupName, String counterName) {
        return groupName + "." + counterName;
    }
    
    public static boolean isCounterKeyInGroup(String counterKey, String groupName) {
        return (counterKey.startsWith(getCounterKey(groupName, "")));
    }
    
    public static String getCounterNameFromCounterKey(String counterKey, String groupName) {
        String groupCounterKeyPrefix = getCounterKey(groupName, "");
        if (counterKey.startsWith(groupCounterKeyPrefix)) {
            int prefixLength = groupCounterKeyPrefix.length();
            return counterKey.substring(prefixLength);
        }
        return null;
    }
    
    // TODO Use this routine with the above code? Would need to map from Enum to name,
    // compare against what we get back here.
    public static Map<String, Long> getCounters(Flow flow) {
        Map<String, Long> result = new HashMap<String, Long>();
        
        FlowStats stats = flow.getFlowStats();
        List<FlowStepStats> stepStats = stats.getFlowStepStats();

        for (FlowStepStats stepStat : stepStats) {
            Collection<String> counterGroups = stepStat.getCounterGroups();
            for (String counterGroup : counterGroups) {
                Collection<String> counters = stepStat.getCountersFor(counterGroup);
                for (String counter : counters) {
                    long counterValue = stepStat.getCounterValue(counterGroup, counter);
                    String counterKey = getCounterKey(counterGroup, counter);
                    if (result.containsKey(counterKey)) {
                        counterValue += result.get(counterKey);
                    }
                    
                    result.put(counterKey, counterValue);
                }
            }
        }

        return result;
    }
    
}
