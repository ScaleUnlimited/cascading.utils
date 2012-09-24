/**
 * Copyright 2010 TransPac Software, Inc.
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

import java.security.InvalidParameterException;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;

@SuppressWarnings("serial")
public class SplitterAssembly extends SubAssembly {
	private static final String LHS_SUFFIX = "-lhs";
	private static final String RHS_SUFFIX = "-rhs";
	
	private String _baseName;
	
	private enum SplitterCounters {
	    LHS,
	    RHS,
	}
	
    @SuppressWarnings("unchecked")
	private static class SplitterFilter extends BaseOperation<NullContext> implements Filter<NullContext> {
		private BaseSplitter _splitter;
		private boolean _wantLHS;
        private Enum _counter;
	    private transient LoggingFlowProcess _flowProcess;
		
		public SplitterFilter(BaseSplitter splitter, boolean wantLHS, Enum counter) {
			_splitter = splitter;
			_wantLHS = wantLHS;
			_counter = counter;
		}
		
	    @Override
	    public void prepare(FlowProcess flowProcess,
	                        OperationCall<NullContext> operationCall) {
	        super.prepare(flowProcess, operationCall);
	        _flowProcess = new LoggingFlowProcess((HadoopFlowProcess) flowProcess);
	        _flowProcess.addReporter(new LoggingFlowReporter());
	    }
	    
		@Override
		public boolean isRemove(FlowProcess flowProcess, FilterCall<NullContext> filterCall) {
		    boolean result = _splitter.isLHS(filterCall.getArguments()) != _wantLHS;
            if (!result) {
                _flowProcess.increment(_counter, 1);
            }
		    return result;
		}

	    @Override
	    public void cleanup(FlowProcess flowProcess,
	                        OperationCall<NullContext> operationCall) {
	        _flowProcess.dumpCounters();
	        super.cleanup(flowProcess, operationCall);
	    }
	}

    public SplitterAssembly(Pipe inputPipe, BaseSplitter splitter) {
        this(inputPipe, splitter, SplitterCounters.LHS, SplitterCounters.RHS);
    }

    @SuppressWarnings("unchecked")
	public SplitterAssembly(Pipe inputPipe,
                            BaseSplitter splitter,
                            Enum lhsCounter,
                            Enum rhsCounter) {
		_baseName = inputPipe.getName();
        Pipe lhsPipe = new Pipe(_baseName + LHS_SUFFIX, inputPipe);
        lhsPipe = new Each(lhsPipe, new SplitterFilter(splitter, true, lhsCounter));
        
        Pipe rhsPipe = new Pipe(_baseName + RHS_SUFFIX, inputPipe);
        rhsPipe = new Each(rhsPipe, new SplitterFilter(splitter, false, rhsCounter));

        setTails(lhsPipe, rhsPipe);
	}
	
	public Pipe getLHSPipe() {
    	return getTailPipe(_baseName + LHS_SUFFIX);
	}

	public Pipe getRHSPipe() {
    	return getTailPipe(_baseName + RHS_SUFFIX);
	}

    private Pipe getTailPipe(String pipeName) {
        String[] pipeNames = getTailNames();
        for (int i = 0; i < pipeNames.length; i++) {
            if (pipeName.equals(pipeNames[i])) {
                return getTails()[i];
            }
        }
        
        throw new InvalidParameterException("Invalid pipe name: " + pipeName);
    }

}
