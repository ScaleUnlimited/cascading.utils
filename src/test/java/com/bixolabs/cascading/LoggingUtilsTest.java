/**
 * Copyright (c) 2009-2010 TransPac Software, Inc.
 * All rights reserved.
 *
 */
package com.bixolabs.cascading;


import java.util.Properties;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

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
        Assert.assertEquals("cascading=INFO,bixo=TRACE,com.bixolabs=TRACE",
                            logSettings);
        
        props.put("log4j.logger", "org.apache=INFO");
        options.setTraceLogging(false);
        options.setDebugLogging(true);
        LoggingUtils.setLoggingProperties(props, options);
        logSettings = (String)props.get("log4j.logger");
        Assert.assertEquals("org.apache=INFO,cascading=INFO,bixo=DEBUG,com.bixolabs=DEBUG",
                            logSettings);
    }
}
