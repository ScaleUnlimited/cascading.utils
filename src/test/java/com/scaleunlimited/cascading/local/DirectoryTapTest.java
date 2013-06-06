package com.scaleunlimited.cascading.local;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import com.scaleunlimited.cascading.local.DirectoryTap;
import com.scaleunlimited.cascading.local.KryoScheme;

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.local.FileTap;
import cascading.tap.local.TemplateTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class DirectoryTapTest {

    @Test
    public void testAsSource() throws IOException {
        File dir = new File("build/test/DirectoryTapTest/testAsSource/");
        FileUtils.deleteDirectory(dir);
        assertTrue(dir.mkdirs());
        
        final Fields fields = new Fields("key", "value");
        FileTap ft = new FileTap(new KryoScheme(fields), new File(dir, "file1").getAbsolutePath(), SinkMode.REPLACE);
        TupleEntryCollector writer = ft.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("key1", 11));
        writer.add(new Tuple("key1", 12));
        writer.add(new Tuple("key2", 21));
        writer.close();
        
        ft = new FileTap(new KryoScheme(fields), new File(dir, "file2").getAbsolutePath(), SinkMode.REPLACE);
        writer = ft.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("key2", 22));
        writer.add(new Tuple("key3", 31));
        writer.close();
        
        DirectoryTap dt = new DirectoryTap(new KryoScheme(fields), dir.getAbsolutePath());
        TupleEntryIterator iter = dt.openForRead(new LocalFlowProcess());
        
        assertEquals(new Tuple("key1", 11), iter.next().getTuple());
        assertEquals(new Tuple("key1", 12), iter.next().getTuple());
        assertEquals(new Tuple("key2", 21), iter.next().getTuple());
        assertEquals(new Tuple("key2", 22), iter.next().getTuple());
        assertEquals(new Tuple("key3", 31), iter.next().getTuple());
        assertFalse(iter.hasNext());
        iter.close();
    }
    
    @Test
    public void testAsSink() throws Exception {
        final String dirPath = "build/test/DirectoryTapTest/testAsSink/";
        
        DirectoryTap dt = new DirectoryTap(new TextLine(), dirPath, SinkMode.REPLACE);
        TupleEntryCollector writer = dt.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("key1", 11));
        writer.add(new Tuple("key1", 12));
        writer.add(new Tuple("key2", 21));
        writer.close();

        // We should have a single file, called part-00000, in the output directory
        File dirFile = new File(dirPath);
        assertTrue(dirFile.exists());
        assertTrue(dirFile.isDirectory());
        
        File resultFile = new File(dirFile, "part-00000");
        assertTrue(resultFile.exists());
        
        List<String> lines = IOUtils.readLines(new FileInputStream(resultFile));
        assertEquals(3, lines.size());
        assertEquals("key1\t11", lines.get(0));
        assertEquals("key1\t12", lines.get(1));
        assertEquals("key2\t21", lines.get(2));
    }

    @Test
    public void testAsTamplateTapSink() throws Exception {
        final String dirPath = "build/test/DirectoryTapTest/testAsTamplateTapSink/in";
        
        DirectoryTap dt = new DirectoryTap(new KryoScheme(new Fields("key", "value")), dirPath, SinkMode.REPLACE);
        TupleEntryCollector writer = dt.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("key1", 11));
        writer.add(new Tuple("key1", 12));
        writer.add(new Tuple("key2", 21));
        writer.close();

        // We should have a single file, called part-00000, in the directory.
        // We'll use that as input, and use TemplateTap for the output.
        Pipe p = new Pipe("pipe");
        
        final String out = "build/test/DirectoryTapTest/testAsTamplateTapSink/out";
        DirectoryTap parentTap = new DirectoryTap(new TextLine(), out, SinkMode.REPLACE);
        TemplateTap sinkTap = new TemplateTap(parentTap, "key-%s", new Fields("key"));
        
        Flow<?> f= new LocalFlowConnector().connect(dt, sinkTap, p);
        f.complete();
        
        // We should have two files, called "key-key1" and "key-key2", in the output directory.
        
        File outDir = new File(out);
        assertTrue(outDir.exists());
        assertTrue(outDir.isDirectory());
        
        File resultFile = new File(outDir, "key-key1");
        assertTrue(resultFile.exists());
        
        List<String> lines = IOUtils.readLines(new FileInputStream(resultFile));
        assertEquals(2, lines.size());
        assertEquals("key1\t11", lines.get(0));
        assertEquals("key1\t12", lines.get(1));
        
        resultFile = new File(outDir, "key-key2");
        assertTrue(resultFile.exists());
        
        lines = IOUtils.readLines(new FileInputStream(resultFile));
        assertEquals(1, lines.size());
        assertEquals("key2\t21", lines.get(0));
    }

    @Test
    public void testWithOneInputFile() throws Exception {
        final String dirPath = "build/test/DirectoryTapTest/testWithOneInputFile/";
        
        DirectoryTap outTap = new DirectoryTap(new TextLine(), dirPath, SinkMode.REPLACE);
        TupleEntryCollector writer = outTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("key1", 11));
        writer.add(new Tuple("key2", 21));
        writer.close();

        // We should have a single file, called part-00000, in the output directory
        File dirFile = new File(dirPath);
        File resultFile = new File(dirFile, "part-00000");

        DirectoryTap inTap = new DirectoryTap(new TextLine(), resultFile.getAbsolutePath(), SinkMode.KEEP);
        TupleEntryIterator iter = inTap.openForRead(new LocalFlowProcess());
        
        assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        assertEquals("key1\t11", te.getString("line"));
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("key2\t21", te.getString("line"));
        
        assertFalse(iter.hasNext());
        iter.close();
    }

    @Test
    public void testIgnoreCRCFiles() throws Exception {
        final String dirPath = "build/test/DirectoryTapTest/testIgnoreCRCFiles/";
        
        DirectoryTap outTap = new DirectoryTap(new TextLine(), dirPath, SinkMode.REPLACE);
        TupleEntryCollector writer = outTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("key1", 11));
        writer.add(new Tuple("key2", 21));
        writer.close();

        // We should have a single file, called part-00000, in the output directory.
        // Let's add a file called .part-00000.crc
        File dirFile = new File(dirPath);
        File crcFile = new File(dirFile, ".part-00000.crc");
        crcFile.createNewFile();
        
        DirectoryTap inTap = new DirectoryTap(new TextLine(), dirPath, SinkMode.KEEP);
        assertEquals(1, inTap.getNumChildTaps());
        
        TupleEntryIterator iter = inTap.openForRead(new LocalFlowProcess());
        
        assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        assertEquals("key1\t11", te.getString("line"));
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("key2\t21", te.getString("line"));
        
        assertFalse(iter.hasNext());
        iter.close();
    }

}
