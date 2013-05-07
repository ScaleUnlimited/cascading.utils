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

import java.util.Map;

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
     * @param myLevel Logging level for our code (not Hadoop/Cascading)
     */
    public static void setLoggingProperties(Map<Object, Object> props, Level myLevel) {
        Level cascadingLevel = Level.INFO;
        Level bixoLevel = myLevel;
        
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
                                                myLevel);
        props.put("log4j.logger", newLogSettings);
        
        // HACK - set the TupleLogger's logging level explicitly, so that subsequent
        // calls to TupleLogger.makePipe() will do the right thing when defining the
        // workflow.
        TupleLogger.setLevel(myLevel);
    }

    /**
     * Set the log level for the com.bixolabs classes, based on <options>
     * 
     * @param props Properties to modify/update.
     * @param options Tool options that provide log level info
     */
    public static void setLoggingProperties(Map<Object, Object> props, BaseOptions options) {
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
