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


import java.util.Properties;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.scaleunlimited.cascading.BaseOptions;
import com.scaleunlimited.cascading.LoggingUtils;

public class LoggingUtilsTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testSetLoggingProperties() {
        Properties props = new Properties();
        BaseOptions options = new BaseOptions();
        
        options.setTraceLogging(true);
        LoggingUtils.setLoggingProperties(props, options);
        String logSettings = (String)props.get("log4j.logger");
        Assert.assertEquals("cascading=INFO,bixo=TRACE,com.bixolabs=TRACE,com.scaleunlimited=TRACE",
                            logSettings);
        
        props.put("log4j.logger", "org.apache=INFO");
        options.setTraceLogging(false);
        options.setDebugLogging(true);
        LoggingUtils.setLoggingProperties(props, options);
        logSettings = (String)props.get("log4j.logger");
        Assert.assertEquals("org.apache=INFO,cascading=INFO,bixo=DEBUG,com.bixolabs=DEBUG,com.scaleunlimited=DEBUG",
                            logSettings);
    }
}
