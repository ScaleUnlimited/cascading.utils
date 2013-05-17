package com.scaleunlimited.cascading.hadoop.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.BasePath;

public class MiniClusterPlatformTest {

    private static final String OUTPUT_DIR = "build/test/MiniClusterPlatformTest/";

    @Before
    public void setup() throws IOException {
        File dataDir = new File("build/test/data");
        if (dataDir.exists()) {
            FileUtils.deleteDirectory(dataDir);
        }
        File mapredDir = new File("build/test/mapred");
        if (mapredDir.exists()) {
            FileUtils.deleteDirectory(mapredDir);
        }

        File outputDir = new File("OUTPUT_DIR");
        if (outputDir.exists()) {
            FileUtils.deleteDirectory(outputDir);
        }
    }
    
    
    @Test
    public void testFullConstructor() throws Exception {
        final String logDirname = "build/test/MiniClusterPlatformTest/log";
        final String tmpDirname = "build/test/MiniClusterPlatformTest/tmp";
        
        MiniClusterPlatform platform = new MiniClusterPlatform(MiniClusterPlatformTest.class, 
                        2, 2, logDirname, tmpDirname);
        platform.setJobPollingInterval(10);

        Flow flow = makeFlow(platform, "testFullConstructor");
        flow.complete();
        
        File logDir = new File(logDirname);
        assertTrue(logDir.exists());
        assertTrue(logDir.isDirectory());

        File tmpDir = new File(tmpDirname);
        assertTrue(tmpDir.exists());
        assertTrue(tmpDir.isDirectory());

        platform.shutdown();
    }
    
//    @Test
    public void testMinConstructor() throws Exception {
        MiniClusterPlatform platform = new MiniClusterPlatform(MiniClusterPlatformTest.class);
        platform.setJobPollingInterval(10);

        Flow flow = makeFlow(platform, "testMinConstructor");
        flow.complete();
        
        platform.shutdown();

    }

    
    private Flow makeFlow(MiniClusterPlatform platform, String testName) throws Exception {
        BasePath path = platform.makePath(OUTPUT_DIR);
        
        BasePath testDir = platform.makePath(path, testName);
        BasePath in = platform.makePath(testDir, "in");
        
        Tap sourceTap = platform.makeTap(platform.makeBinaryScheme(new Fields("user", "val")), in, SinkMode.REPLACE);
        TupleEntryCollector write = sourceTap.openForWrite(platform.makeFlowProcess());
        int i = 0;
        while (i < 10) {
            String username = "user-" + i;
            write.add(new Tuple(username, i));
            i++;
        }
        write.close();

        Pipe pipe = new Pipe("test");
        
        BasePath out = platform.makePath(testDir, "out");
        Tap sinkTap = platform.makeTap(platform.makeTextScheme(), out, SinkMode.REPLACE);

        Flow flow = platform.makeFlowConnector().connect(testName, sourceTap, sinkTap, pipe);
        return flow;
    }
}
