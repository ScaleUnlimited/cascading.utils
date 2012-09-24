package com.scaleunlimited.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;

@SuppressWarnings("serial")
public class FlowBreak extends SubAssembly {

    private static class FlowBreaker extends BaseOperation<NullContext> implements Filter<NullContext> {

        @Override
        public boolean isSafe() {
            // Force Cascading to save results to TempHFS
            return false;
        }
        
        @Override
        public boolean isRemove(FlowProcess arg0, FilterCall<NullContext> arg1) {
            return false;
        }
    }
    
    public FlowBreak(Pipe p) {
        p = new Each(p, new FlowBreaker());
        setTails(p);
    }
}
