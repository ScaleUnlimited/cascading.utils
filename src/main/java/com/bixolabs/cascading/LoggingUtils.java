/**
 * Copyright (c) 2009-2011 TransPac Software, Inc.
 * All rights reserved.
 *
 */
package com.bixolabs.cascading;

import java.util.Properties;

import org.apache.log4j.Level;

/**
 * Leverage Cascading's support for setting Log4J properties during map/reduce jobs,
 * by setting the special "log4j.logger" property
 *
 */
public class LoggingUtils {

    /**
     * Set the log level for the com.bixolabs classes to be <level>
     * 
     * @param props Properties to modify/update.
     * @param bixoLabsLevel Logging level for our code (not Hadoop/Cascading)
     */
    public static void setLoggingProperties(Properties props, Level bixoLabsLevel) {
        Level cascadingLevel = Level.INFO;
        Level bixoLevel = bixoLabsLevel;
        
        String curLogSettings = (String)props.get("log4j.logger");
        if  (   (curLogSettings == null)
            ||  (curLogSettings.trim().isEmpty())) {
            curLogSettings = "";
        } else {
            curLogSettings = curLogSettings + ",";
        }
        String newLogSettings = String.format(  "%scascading=%s,bixo=%s,com.bixolabs=%s",
                                                curLogSettings,
                                                cascadingLevel,
                                                bixoLevel,
                                                bixoLabsLevel);
        props.put("log4j.logger", newLogSettings);
    }

    /**
     * Set the log level for the com.bixolabs classes, based on <options>
     * 
     * @param props Properties to modify/update.
     * @param options Tool options that provide log level info
     */
    public static void setLoggingProperties(Properties props, BaseOptions options) {
    	Level ourLevel;
    	
        if (options.isTraceLogging()) {
        	ourLevel = Level.TRACE;
        } else if (options.isDebugLogging()) {
        	ourLevel = Level.DEBUG;
        } else {
        	ourLevel = Level.INFO;
        }
        
        setLoggingProperties(props, ourLevel);
    }

}
