/**
 * Copyright 2010-2012 TransPac Software, Inc.
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

import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.NamingFlowStep;
import cascading.operation.Operation;
import cascading.pipe.Group;
import cascading.stats.FlowStepStats;

public class StepUtils {

    private static final Pattern DEFAULT_OPERATION_NAME_PATTERN =
        Pattern.compile("(.+)\\[.+\\]");

    public static long safeGetCounter(FlowStepStats stepStats, Enum<?> counter) {
        try {
            return stepStats.getCounterValue(counter);
        } catch (NullPointerException e) {
            // Catch case of job having ended, so stepStats.getRunningJob() returns
            // null, but Cascading doesn't check for this and tries to get the counter,
            // resulting in a NPE.
            return 0;
        }
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void nameFlowStep(BaseFlowStep step) {
        Group group = step.getGroup();
        
        String stepName = "";
        if (group == null) {
            Collection<Operation> operations = step.getAllOperations();
            for (Operation operation : operations) {
                String operationName = operation.toString();
                Matcher defaultNameMatcher = DEFAULT_OPERATION_NAME_PATTERN.matcher(operationName);
                if (defaultNameMatcher.matches()) {
                    operationName = defaultNameMatcher.group(1);
                }
                stepName = stepName + operationName + "+";
            }
            
            if (operations.size() > 0) {
                stepName = stepName.substring(0, stepName.length()-1);
            }
        } else {
            stepName = group.getName();
        }
        
        // setName exists, but it's protected. So we use our special class that's in the
        // same package, to work around this.
        NamingFlowStep.setName(step, stepName);
    }
}
