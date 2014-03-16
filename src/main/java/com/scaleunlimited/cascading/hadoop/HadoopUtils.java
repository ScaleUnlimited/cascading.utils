/**
 * Copyright 2010-2011 TransPac Software, Inc.
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

package com.scaleunlimited.cascading.hadoop;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.flow.FlowProcessWrapper;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.property.AppProps;

import com.scaleunlimited.cascading.Level;
import com.scaleunlimited.cascading.LoggingFlowProcess;

public class HadoopUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopUtils.class);
    
    private static final long STATUS_CHECK_INTERVAL = 10000;
	
    public static void safeRemove(FileSystem fs, Path path) {
    	if ((fs != null) && (path != null)) {
    		try {
    			fs.delete(path, true);
    		} catch (Throwable t) {
    			// Ignore
    		}
    	}
    }
    
    /**
     * Return the number of reducers, and thus the max number of parallel reduce tasks.
     * 
     * @param conf
     * @return number of reducers
     * @throws IOException
     * @throws InterruptedException
     */
    public static int getNumReducers(JobConf conf) throws IOException, InterruptedException {
        ClusterStatus status = safeGetClusterStatus(conf);
        return status.getMaxReduceTasks();
    }
    
    public static int getTaskTrackers(JobConf conf) throws IOException, InterruptedException {
        ClusterStatus status = safeGetClusterStatus(conf);
        return status.getTaskTrackers();
    }
    
    public static JobConf getDefaultJobConf() throws IOException, InterruptedException {
        JobConf conf = new JobConf();
        
        // We explicitly set task counts to 1 for local so that code which depends on
        // things like the reducer count runs properly.
        if (isJobLocal(conf)) {
            conf.setNumMapTasks(1);
            conf.setNumReduceTasks(1);
        } else {
            conf.setNumReduceTasks(getNumReducers(conf));

            // TODO - By default we want to use 0.95 * the number of reduce slots, as per
            // Hadoop wiki. But we want to round, versus truncate, to avoid setting it to
            // 0 if we have one reducer. This way it only impacts you if you have more
            // than 10 reducers.
            // conf.setNumReduceTasks((getNumReducers(conf) * 95) / 100);
        }
        
        conf.setMapSpeculativeExecution(false);
        conf.setReduceSpeculativeExecution(false);

        return conf;
    }

    public static void setLoggingProperties(Properties props, Level cascadingLevel, Level bixoLevel) {
    	props.put("log4j.logger", String.format("cascading=%s,bixo=%s", cascadingLevel, bixoLevel));
    }
    
	public static Map<Object, Object> getDefaultProperties(Class appJarClass, boolean debugging, JobConf conf) {
        Map<Object, Object> properties = HadoopUtil.createProperties(conf);

        // Use special Cascading hack to control logging levels for code running as Hadoop jobs
        if (debugging) {
            properties.put("log4j.logger", "cascading=DEBUG,bixo=TRACE");
        } else {
            properties.put("log4j.logger", "cascading=INFO,bixo=INFO");
        }

        AppProps.setApplicationJarClass(properties, appJarClass);

        return properties;
    }
    
    @SuppressWarnings("deprecation")
    public static boolean isJobLocal(JobConf conf) {
        return conf.get( "mapred.job.tracker" ).equalsIgnoreCase( "local" );
    }
    
    @SuppressWarnings("rawtypes")
    public static boolean isHadoopFlowProcess(FlowProcess fp) {
        return (undelegate(fp) instanceof HadoopFlowProcess);
    }
    
    @SuppressWarnings("rawtypes")
    public static FlowProcess undelegate(FlowProcess fp) {
        FlowProcess delegate = fp;
        if (delegate instanceof LoggingFlowProcess) {
            delegate = ((LoggingFlowProcess)delegate).getDelegate();
        }
        int delegateNestingLevel = 0;
        while (delegate instanceof FlowProcessWrapper) {
            if (++delegateNestingLevel > 100) {
                throw new RuntimeException("FlowProcessWrapper seems to have circular nesting references");
            }
            delegate = FlowProcessWrapper.undelegate(delegate);
        }
        return delegate;
    }
    
    /**
     * Utility routine that tries to ensure the cluster is "stable" (slaves have reported in) so
     * that it's safe to call things like maxReduceTasks.
     * 
     * @param conf
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @SuppressWarnings("deprecation")
    private static ClusterStatus safeGetClusterStatus(JobConf conf) throws IOException, InterruptedException {
        JobClient jobClient = new JobClient(conf);
        int numTaskTrackers = -1;
        
        while (true) {
            ClusterStatus status = jobClient.getClusterStatus();
            if (status.getJobTrackerState() == State.RUNNING) {
                int curTaskTrackers = status.getTaskTrackers();
                if (curTaskTrackers == numTaskTrackers) {
                    return status;
                } else {
                    // Things are still settling down, so keep looping.
                    if (numTaskTrackers != -1) {
                        LOGGER.trace(String.format("Got incremental update to number of task trackers (%d to %d)", numTaskTrackers, curTaskTrackers));
                    }
                    
                    numTaskTrackers = curTaskTrackers;
                }
            }
            
            if (!isJobLocal(conf)) {
                LOGGER.trace("Sleeping during status check");
                Thread.sleep(STATUS_CHECK_INTERVAL);
            }
        }
    }


}
