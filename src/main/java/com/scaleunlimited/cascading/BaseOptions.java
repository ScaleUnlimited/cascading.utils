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

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.log4j.Level;
import org.kohsuke.args4j.Option;

public class BaseOptions {
    private boolean _debugLogging = false;
    private boolean _traceLogging = false;
    
    private String _dotFile;

    @Option(name = "-debug", usage = "debug logging", required = false)
    public void setDebugLogging(boolean debugLogging) {
        _debugLogging = debugLogging;
    }

    @Option(name = "-trace", usage = "trace logging", required = false)
    public void setTraceLogging(boolean traceLogging) {
        _traceLogging = traceLogging;
    }

    @Option(name = "-dotfile", usage = "path/name of DOT file", required = false)
    public void setDOTFile(String dotFile) {
        _dotFile = dotFile;
    }

    public String getDOTFile() {
        return _dotFile;
    }
    
    public boolean isDebugLogging() {
        return _debugLogging;
    }
    
    public boolean isTraceLogging() {
        return _traceLogging;
    }
    
    public Level getLogLevel() {
        if (isTraceLogging()) {
            return Level.TRACE;
        } else if (isDebugLogging()) {
            return Level.DEBUG;
        } else {
            return Level.INFO;
        }
    }
    
    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

}
