package com.bixolabs.cascading;

import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.stats.StepStats;

public interface IMonitorTask {

    public String getName(Flow flow, FlowStep flowStep);
    
    public String getValue(Flow flow, FlowStep flowStep, StepStats stepStats);
}
