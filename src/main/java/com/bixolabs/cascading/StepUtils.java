package com.bixolabs.cascading;

import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cascading.flow.FlowStep;
import cascading.operation.Operation;
import cascading.pipe.Group;
import cascading.stats.StepStats;

public class StepUtils {

    private static final Pattern DEFAULT_OPERATION_NAME_PATTERN =
        Pattern.compile("(.+)\\[.+\\]");

    @SuppressWarnings("unchecked")
    public static long safeGetCounter(StepStats stepStats, Enum counter) {
        try {
            return stepStats.getCounterValue(counter);
        } catch (NullPointerException e) {
            // Catch case of job having ended, so stepStats.getRunningJob() returns
            // null, but Cascading doesn't check for this and tries to get the counter,
            // resulting in a NPE.
            return 0;
        }
    }
    
    @SuppressWarnings("unchecked")
    public static void nameFlowStep(FlowStep step) {
        
        Group group = step.getGroup();
        
        // Here's a version of the line above that works with Cascading 1.1.1:
        //
        // Group group = step.group;
        
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
        step.setParentFlowName(stepName);
    }
}
