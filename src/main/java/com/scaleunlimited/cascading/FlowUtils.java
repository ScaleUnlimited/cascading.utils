package com.scaleunlimited.cascading;

import java.util.List;

import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.flow.planner.BaseFlowStep;
import cascading.stats.CascadingStats.Status;
import cascading.stats.FlowStepStats;

public class FlowUtils {

    /**
     * Work around a bug where Cascading throws an exception if we try to stop a flow
     * that isn't yet fully started (state == PENDING).
     * 
     * @param flow
     * @throws InterruptedException 
     */
    public static void safeStop(Flow flow) {
        boolean potentialBug = flow.getFlowStats().getStatus() == Status.PENDING;
        
        try {
            flow.stop();
        } catch (IllegalStateException e) {
            if (potentialBug) {
                while (flow.getFlowStats().getStatus() == Status.PENDING) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                    }
                }
                
                flow.stop();
            } else {
                throw e;
            }
        }
    }
    
    public static void nameFlowSteps(Flow flow) {
        List<FlowStep> steps = flow.getFlowSteps();
        
        for (FlowStep step : steps) {
            if (step instanceof BaseFlowStep) {
                BaseFlowStep baseStep = (BaseFlowStep)step;
                StepUtils.nameFlowStep(baseStep);
            }
        }
    }

}
