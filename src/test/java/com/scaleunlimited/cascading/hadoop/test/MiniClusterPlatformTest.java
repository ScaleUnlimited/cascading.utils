package com.scaleunlimited.cascading.hadoop.test;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.scaleunlimited.cascading.BasePath;

import cascading.flow.Flow;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class MiniClusterPlatformTest {

    private static final String BASE_DIR = "build/test/MiniClusterPlatformTest/";

    private MiniClusterPlatform _platform;
    
    @Before
    public void setUp() throws IOException {
        _platform = null;
        
        File outputDir = new File(BASE_DIR);
        if (outputDir.exists()) {
            FileUtils.deleteDirectory(outputDir);
        }
    }
    
    @After
    public void tearDown() throws InterruptedException, IOException {
        if (_platform != null) {
            _platform.shutdown();
            _platform = null;
        }
    }
    
    @Test
    public void testFullConstructor() throws Exception {
        _platform = new MiniClusterPlatform(MiniClusterPlatformTest.class, 2);

        Flow flow = makeFlow("testFullConstructor1");
        flow.complete();
        
        // TODO currently this will fail, because we can't control where the mini cluster puts its logs.
//        File logDir = new File(logDirname);
//        assertTrue(logDir.exists());
//        assertTrue(logDir.isDirectory());
    }
    
    // TODO currently this test will fail, because you can't run the mini cluster in
    // the same JVM twice in a row, because shutting it down doesn't really shut things
    // down properly.
    
    // @Test
    public void testMinConstructor() throws Exception {
        _platform = new MiniClusterPlatform(MiniClusterPlatformTest.class);
        Flow flow = makeFlow("testMinConstructor");
        flow.complete();
    }

    
    private Flow makeFlow(String testName) throws Exception {
        BasePath testDir = _platform.makePath(testName);
        BasePath in = _platform.makePath(testDir, "in");
        
        Tap sourceTap = _platform.makeTap(_platform.makeBinaryScheme(new Fields("user", "val")), in, SinkMode.REPLACE);
        TupleEntryCollector write = sourceTap.openForWrite(_platform.makeFlowProcess());
        int i = 0;
        while (i < 10) {
            String username = "user-" + i;
            write.add(new Tuple(username, i));
            i++;
        }
        write.close();

        Pipe pipe = new Pipe("test");
        
        BasePath out = _platform.makePath(testDir, "out");
        Tap sinkTap = _platform.makeTap(_platform.makeTextScheme(), out, SinkMode.REPLACE);

        Flow flow = _platform.makeFlowConnector().connect(testName, sourceTap, sinkTap, pipe);
        return flow;
    }
}
