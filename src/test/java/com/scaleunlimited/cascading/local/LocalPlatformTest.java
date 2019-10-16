package com.scaleunlimited.cascading.local;

import java.io.File;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.scaleunlimited.cascading.AbstractPlatformTest;
import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class LocalPlatformTest extends AbstractPlatformTest {

    private static final String WORKING_DIR = "build/test/LocalPlatformTest";
    
    @Before
    public void setUp() {
        File workingDir = new File(WORKING_DIR);
        if (workingDir.exists()) {
            FileUtils.deleteQuietly(workingDir);
        }
        workingDir.mkdirs();
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testTempPath() throws Exception {
        BasePlatform platform = new LocalPlatform(LocalPlatformTest.class);
        
        BasePath tempDir = platform.getTempDir();
        
        // Verify we can write and then read
        BasePath testDir = platform.makePath(tempDir, UUID.randomUUID().toString());
        
        Scheme scheme = platform.makeBinaryScheme(new Fields("name", "age"));
        Tap tap = platform.makeTap(scheme, testDir);
        TupleEntryCollector writer = tap.openForWrite(platform.makeFlowProcess());
        writer.add(new Tuple("ken", 37));
        writer.close();

        TupleEntryIterator iter = tap.openForRead(platform.makeFlowProcess());
        assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        assertEquals("ken", te.getString("name"));
        assertFalse(iter.hasNext());
        iter.close();
    }
    
    @Test
    public void testPathCreation() throws Exception {
        // Clear it out first.
        final String targetDirname = "build/test/LocalPlatformTest/testPathCreation";
        File targetDirFile = new File(targetDirname);
        FileUtils.deleteDirectory(targetDirFile);
        assertFalse(targetDirFile.exists());
        
        BasePlatform platform = new LocalPlatform(LocalPlatformTest.class);
        
        BasePath path = platform.makePath(targetDirname);
        assertEquals(targetDirname, path.getPath());
        assertEquals(targetDirFile.getAbsolutePath(), path.getAbsolutePath());
        assertEquals(targetDirFile.getAbsolutePath(), path.toString());
        
        assertFalse(path.exists());
        assertTrue(path.mkdirs());
        assertTrue(path.isDirectory());
        assertFalse(path.isFile());

        assertTrue(targetDirFile.exists());
        assertTrue(targetDirFile.isDirectory());

        // Check out sub-dir support.
        File subDirFile = new File(targetDirFile, "subdir");

        BasePath child = platform.makePath(path, "subdir");
        assertEquals("subdir", child.getPath());
        assertEquals(subDirFile.getAbsolutePath(), child.getAbsolutePath());
        
        assertFalse(child.exists());
        assertTrue(child.mkdirs());
        assertTrue(child.isDirectory());
        assertFalse(child.isFile());
        
        assertTrue(subDirFile.exists());
        assertTrue(subDirFile.isDirectory());
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testBinaryScheme() throws Exception {
        BasePlatform platform = new LocalPlatform(LocalPlatformTest.class);
        final String targetDirname = "build/test/LocalPlatformTest/testPathCreation";
        BasePath path = platform.makePath(targetDirname);

        Scheme scheme = platform.makeBinaryScheme(new Fields("name", "age"));
        Tap tap = platform.makeTap(scheme, path);
        TupleEntryCollector writer = tap.openForWrite(platform.makeFlowProcess());
        writer.add(new Tuple("ken", 37));
        writer.close();
    }

    @Test
    public void testRename() throws Exception {
        BasePlatform platform = new LocalPlatform(LocalPlatformTest.class);
        final String targetDirname = "build/test/LocalPlatformTest/testRename";
        BasePath path = platform.makePath(targetDirname);
        if (path.exists()) {
            path.delete(true);
        }
        path.mkdirs();
        
        BasePath src = platform.makePath(path, "src");
        src.mkdirs();
        
        assertTrue(src.exists());
        
        BasePath dst = platform.makePath(path, "dst");
        assertFalse(dst.exists());
        platform.rename(src, dst);
        
        assertTrue(dst.exists());
        assertFalse(src.exists());
    }
    
    @Test
    public void testSerialization() throws Exception {
        BasePlatform platform = new LocalPlatform(LocalPlatformTest.class);
        
        testSerialization(platform);
    }

    @Test
    public void testPlatformType() throws Exception {
        BasePlatform platform = new LocalPlatform(LocalPlatformTest.class);
        assertEquals(LocalPlatform.PLATFORM_TYPE, platform.getPlatformType());
    }
    
    @SuppressWarnings("rawtypes")
    @Test
    public void testPartitionTap() throws Exception {
        
        BasePlatform platform = new LocalPlatform(LocalPlatformTest.class);
        BasePath workingDir = platform.makePath(WORKING_DIR);
        BasePath testDir = platform.makePath(workingDir, "testPartitionTap");
        BasePath input = platform.makePath(testDir, "input");
        input.mkdirs();
        
        // Make two month-year directories
        createDataDir(platform, input, "07-2014", "input1 test");
        createDataDir(platform, input, "08-2014", "input2 test");
         
        
        // create a flow to read from the input
        Pipe inputPipe = new Pipe("input");
        Tap parentSourceTap = platform.makeTap(platform.makeTextScheme(), input);
        DelimitedPartition monthYearPartition = new DelimitedPartition( new Fields( "month", "year" ), "-" );
        Tap monthYearTap = platform.makePartitionTap(parentSourceTap, monthYearPartition);
        
        // and write to output - but as year-month
        BasePath output = platform.makePath(testDir, "output");
        Tap parentSinkTap = platform.makeTap(platform.makeTextScheme(), output);
        DelimitedPartition yearMonthPartition = new DelimitedPartition( new Fields( "year", "month" ), "-" );
        Tap yearMonthTap = platform.makePartitionTap(parentSinkTap, yearMonthPartition, SinkMode.REPLACE);
        
        FlowDef flowDef = new FlowDef()
                       .setName("Local PartitionTap Test")
                       .addSource(inputPipe, monthYearTap)
                       .addTailSink(inputPipe, yearMonthTap);
        Flow flow = platform.makeFlowConnector().connect(flowDef);
        flow.complete();
        // TODO  verify that input and output exist
        
    }
    
    @SuppressWarnings("rawtypes")
    @Test
    public void testPartitionTapWithSlashes() throws Exception {
        
        BasePlatform platform = new LocalPlatform(LocalPlatformTest.class);
        BasePath workingDir = platform.makePath(WORKING_DIR);
        BasePath testDir = platform.makePath(workingDir, "testPartitionTapWithSlashes");
        BasePath input = platform.makePath(testDir, "input");
        input.mkdirs();
        
        // Make two month-year directories
        createDataDir(platform, input, "07/2014", "input1 test");
        createDataDir(platform, input, "08/2014", "input2 test");
         
        
        // create a flow to read from the input
        Pipe inputPipe = new Pipe("input");
        Tap parentSourceTap = platform.makeTap(platform.makeTextScheme(), input);
        DelimitedPartition monthYearPartition = new DelimitedPartition( new Fields( "month", "year" ), "/" );
        Tap monthYearTap = platform.makePartitionTap(parentSourceTap, monthYearPartition);
        
        // and write to output - but as year-month
        BasePath output = platform.makePath(testDir, "output");
        Tap parentSinkTap = platform.makeTap(platform.makeTextScheme(), output);
        DelimitedPartition yearMonthPartition = new DelimitedPartition( new Fields( "year", "month" ), "/" );
        Tap yearMonthTap = platform.makePartitionTap(parentSinkTap, yearMonthPartition, SinkMode.REPLACE);
        
        FlowDef flowDef = new FlowDef()
                       .setName("Local PartitionTap Test")
                       .addSource(inputPipe, monthYearTap)
                       .addTailSink(inputPipe, yearMonthTap);
        Flow flow = platform.makeFlowConnector().connect(flowDef);
        flow.complete();
        // TODO  verify that input and output exist
        
        
    }

    /**
     * We need to create the input data we're using, which requires that we use an explicit FileTap, as that's
     * what a PartitionTap needs, and thus we have to write out data as files, not <path to directory>/part-00000
     * 
     * @param platform
     * @param input
     * @param dirName
     * @param data
     * @throws Exception
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void createDataDir(BasePlatform platform, BasePath input, String dirName, String data) throws Exception {
        BasePath destPath = platform.makePath(input, dirName);
        Tap tap = new FileTap(platform.makeTextScheme(), destPath.getAbsolutePath());
        TupleEntryCollector tupleEntryCollector = tap.openForWrite(platform.makeFlowProcess());
        tupleEntryCollector.add(new Tuple(data));
        tupleEntryCollector.close();
    }

}
