/**
 * Copyright 2010-2013 Scale Unlimited.
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

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.Flow;

public class BaseTool {

    protected static void printUsageAndExit(CmdLineParser parser) {
        parser.printUsage(System.err);
        System.exit(-1);
    }
    
    private static String getBaseDotFileName(BaseOptions options) {
        String dotFileName = options.getDOTFile();
        int suffixOffset = dotFileName.lastIndexOf('.');
        if ((suffixOffset != -1) && (suffixOffset >= dotFileName.length() - 6)) {
            return dotFileName.substring(0, suffixOffset);
        } else {
            return dotFileName;
        }
    }

    protected static String getStepDotFileName(BaseOptions options, String suffix) {
        return getBaseDotFileName(options) + "-" + suffix + "-step.dot";
    }
    
    protected static String getDotFileName(BaseOptions options, String suffix) {
        return getBaseDotFileName(options) + "-" + suffix + ".dot";
    }
    
    /**
     * Name the steps (jobs) in the flow, using the operation (or pipe) names
     * Use FlowUtils.nameFlowSteps instead.
     * 
     * @param flow
     */
    @Deprecated
    @SuppressWarnings("rawtypes")
    protected static void nameFlowSteps(Flow flow) {
        FlowUtils.nameFlowSteps(flow);
    }
    
    protected static CmdLineParser parse(String[] args, BaseOptions options) {
        CmdLineParser parser = new CmdLineParser(options);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            printUsageAndExit(parser);
        }
        
        // We can't dynamically change the slf4j log level, so we'll just have
        // to set system properties that will impact Log4J levels (in case the
        // user of cascading.utils is using log4j).
        LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        if (options.isTraceLogging()) {
            System.setProperty("my.root.level", "TRACE");
            System.setProperty("my.cascading.level", "TRACE");
        } else if (options.isDebugLogging()) {
            System.setProperty("my.root.level", "DEBUG");
        } else {
            System.setProperty("my.root.level", "INFO");
        }
        
        return parser;
    }
    
}
