/**
 * Copyright (c) 2009-2010 TransPac Software, Inc.
 * All rights reserved.
 *
 */
package com.bixolabs.cascading;

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
