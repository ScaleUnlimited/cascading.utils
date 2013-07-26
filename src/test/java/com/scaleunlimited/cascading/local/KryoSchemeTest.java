package com.scaleunlimited.cascading.local;

import static org.junit.Assert.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import com.scaleunlimited.cascading.TupleLogger;
import com.scaleunlimited.cascading.local.KryoScheme;

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.SumBy;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class KryoSchemeTest {

    private static class MyIntClass {
        private int _value;
        
        public MyIntClass(int value) {
            _value = value;
        }
        
        public int getValue() {
            return _value;
        }
    }
    
    private static class MyWritable implements Writable {
        private Object _value;
        
        public MyWritable() {
            // Empty constructor for Writable (not that it gets called w/Kryo)
        }
        
        public MyWritable(int value) {
            _value = new Integer(value);
        }

        public int getValue() {
            return (Integer)_value;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            _value = new Integer(in.readInt());
            
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt((Integer)_value);
        }
    }
    
    @Test
    public void testSimple() throws Exception {
        final String targetDir = "build/test/KryoSchemeTest/testSimple";
        
        // Create a local tap that uses the KryoScheme
        Fields fields = new Fields("key", "value");
        
        Tap out = new FileTap(new KryoScheme(fields), targetDir);
        TupleEntryCollector writer = out.openForWrite(new LocalFlowProcess());
        
        writer.add(new Tuple("key1", "value11"));
        writer.add(new Tuple("key1", "value12"));
        writer.add(new Tuple("key2", "value21"));
        writer.close();
        
        Tap in = new FileTap(new KryoScheme(fields), targetDir);
        TupleEntryIterator iter = in.openForRead(new LocalFlowProcess());
        
        assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        assertEquals("key1", te.getString("key"));
        assertEquals("value11", te.getString("value"));
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("key1", te.getString("key"));
        assertEquals("value12", te.getString("value"));

        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("key2", te.getString("key"));
        assertEquals("value21", te.getString("value"));

        assertFalse(iter.hasNext());
        
        iter.close();
    }

    @Test
    public void testEmptyFile() throws Exception {
        final String targetDir = "build/test/KryoSchemeTest/testEmptyFile";
        
        // Create a local tap that uses the KryoScheme, but don't write anything.
        // This will create an empty file.
        Fields fields = new Fields("key", "value");
        
        Tap out = new FileTap(new KryoScheme(fields), targetDir);
        TupleEntryCollector writer = out.openForWrite(new LocalFlowProcess());
        writer.close();
        
        Tap in = new FileTap(new KryoScheme(fields), targetDir);
        TupleEntryIterator iter = in.openForRead(new LocalFlowProcess());
        
        assertFalse(iter.hasNext());
        iter.close();
    }

    @Test
    public void testWritable() throws Exception {
        final String targetDir = "build/test/KryoSchemeTest/testWritable";
        
        // Create a local tap that uses the KryoScheme
        Fields fields = new Fields("key", "bytes", "value");
        
        Tap out = new FileTap(new KryoScheme(fields), targetDir);
        TupleEntryCollector writer = out.openForWrite(new LocalFlowProcess());
        
        writer.add(new Tuple("key1", new BytesWritable(new byte[] {1, 1}), new MyWritable(1)));
        writer.add(new Tuple("key1", new BytesWritable(new byte[] {1, 2}), new MyWritable(1)));
        writer.add(new Tuple("key2", new BytesWritable(new byte[] {2, 1}), new MyWritable(2)));
        writer.close();
        
        Tap in = new FileTap(new KryoScheme(fields), targetDir);
        TupleEntryIterator iter = in.openForRead(new LocalFlowProcess());
        
        assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        assertEquals("key1", te.getString("key"));
        BytesWritable bw = (BytesWritable)te.getObject("bytes");
        assertNotNull(bw);
        assertEquals(1, bw.getBytes()[0]);
        assertEquals(1, bw.getBytes()[1]);
        MyWritable mw = (MyWritable)te.getObject("value");
        assertNotNull(mw);
        assertEquals(1, mw.getValue());
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("key1", te.getString("key"));
        bw = (BytesWritable)te.getObject("bytes");
        assertNotNull(bw);
        assertEquals(1, bw.getBytes()[0]);
        assertEquals(2, bw.getBytes()[1]);
        mw = (MyWritable)te.getObject("value");
        assertNotNull(mw);
        assertEquals(1, mw.getValue());
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("key2", te.getString("key"));
        bw = (BytesWritable)te.getObject("bytes");
        assertNotNull(bw);
        assertEquals(2, bw.getBytes()[0]);
        assertEquals(1, bw.getBytes()[1]);
        mw = (MyWritable)te.getObject("value");
        assertNotNull(mw);
        assertEquals(2, mw.getValue());
        
        assertFalse(iter.hasNext());
        
        iter.close();
    }
    
    @Test
    public void testCustomClassNoEmptyConstructor() throws Exception {
        final String targetDir = "build/test/KryoSchemeTest/testCustomClassNoEmptyConstructor";
        
        // Create a local tap that uses the KryoScheme
        Fields fields = new Fields("key", "value");
        
        Tap out = new FileTap(new KryoScheme(fields), targetDir);
        TupleEntryCollector writer = out.openForWrite(new LocalFlowProcess());
        
        writer.add(new Tuple("key1", new MyIntClass(1)));
        writer.add(new Tuple("key1", new MyIntClass(1)));
        writer.add(new Tuple("key2", new MyIntClass(2)));
        writer.close();
        
        Tap in = new FileTap(new KryoScheme(fields), targetDir);
        TupleEntryIterator iter = in.openForRead(new LocalFlowProcess());
        
        assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        assertEquals("key1", te.getString("key"));
        MyIntClass mic = (MyIntClass)te.getObject("value");
        assertNotNull(mic);
        assertEquals(1, mic.getValue());
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("key1", te.getString("key"));
        mic = (MyIntClass)te.getObject("value");
        assertNotNull(mic);
        assertEquals(1, mic.getValue());
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("key2", te.getString("key"));
        mic = (MyIntClass)te.getObject("value");
        assertNotNull(mic);
        assertEquals(2, mic.getValue());
        
        assertFalse(iter.hasNext());
        
        iter.close();
    }
    
    @Test
    public void testInWorkflow() throws Exception {
        final String srcDir = "build/test/KryoSchemeTest/testInWorkflow/src";
        final String dstFile = "build/test/KryoSchemeTest/testInWorkflow/dstFile";
        
        // Create a local tap that uses the KryoScheme
        Fields fields = new Fields("key", "value");
        
        Tap tap = new DirectoryTap(new KryoScheme(fields), srcDir, SinkMode.REPLACE);
        TupleEntryCollector writer = tap.openForWrite(new LocalFlowProcess());
        
        writer.add(new Tuple("key1", 11));
        writer.add(new Tuple("key1", 12));
        writer.add(new Tuple("key2", 21));
        writer.close();

        Tap sourceTap = new DirectoryTap(new KryoScheme(fields), srcDir);
        
        Pipe p = new Pipe("pipe");
        p = new SumBy(p, new Fields("key"), new Fields("value"), new Fields("sum"), Integer.class);
        
        Tap sinkTap = new FileTap(new TextLine(), dstFile, SinkMode.REPLACE);
        Flow f = new LocalFlowConnector().connect(sourceTap, sinkTap, p);
        f.complete();
        
        // TODO verify we have expected output
        Tap validationTap = new FileTap(new TextDelimited(new Fields("key", "sum"), "\t", new Class[] {String.class, Integer.class}), dstFile);
        TupleEntryIterator iter = validationTap.openForRead(new LocalFlowProcess());
        
        assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        assertEquals("key1", te.getString("key"));
        assertEquals(23, te.getInteger("sum"));
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("key2", te.getString("key"));
        assertEquals(21, te.getInteger("sum"));
        
        assertFalse(iter.hasNext());
        
        iter.close();
    }

    @Test
    public void testFieldSelection() throws Exception {
        final String srcDir = "build/test/KryoSchemeTest/testFieldSelection/src";
        final String dstFile = "build/test/KryoSchemeTest/testFieldSelection/dst";
        
        // Create a local tap that uses the KryoScheme
        Fields fields = new Fields("key", "value", "index");
        
        Tap tap = new DirectoryTap(new KryoScheme(Fields.UNKNOWN, fields), srcDir, SinkMode.REPLACE);
        TupleEntryCollector writer = tap.openForWrite(new LocalFlowProcess());
        
        writer.add(new Tuple("key1", 11, 1));
        writer.add(new Tuple("key1", 12, 2));
        writer.add(new Tuple("key2", 21, 3));
        writer.close();

        Tap sourceTap = new DirectoryTap(new KryoScheme(fields), srcDir, SinkMode.KEEP);
        
        Pipe p = new Pipe("pipe");
        
        // Create a sink where we're writing out a subset of the fields, in a different order.
        Tap sinkTap = new DirectoryTap(new KryoScheme(Fields.UNKNOWN, new Fields("index", "value")), dstFile, SinkMode.REPLACE);
        Flow f = new LocalFlowConnector().connect(sourceTap, sinkTap, p);
        f.complete();
        
        // Verify we have expected output
        Tap validationTap = new DirectoryTap(new KryoScheme(new Fields("index", "value")), dstFile);
        TupleEntryIterator iter = validationTap.openForRead(new LocalFlowProcess());
        
        assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        assertEquals(1, te.getInteger("index"));
        assertEquals(11, te.getInteger("value"));
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals(2, te.getInteger("index"));
        assertEquals(12, te.getInteger("value"));
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals(3, te.getInteger("index"));
        assertEquals(21, te.getInteger("value"));
        
        assertFalse(iter.hasNext());
        
        iter.close();
        
        // And that we don't have fields we don't expect to have.
        
        try {
            validationTap = new DirectoryTap(new KryoScheme(fields), dstFile);
            validationTap.openForRead(new LocalFlowProcess());
            fail("Should have thrown exception");
        } catch (Exception e) {
            // expected
        }
        
    }

    @Test
    public void testFieldReordering() throws Exception {
        final String srcDir = "build/test/KryoSchemeTest/testFieldReordering/src";
        final String dstFile = "build/test/KryoSchemeTest/testFieldReordering/dst";
        
        // Create a local tap that uses the KryoScheme
        Fields fields = new Fields("key", "value");
        
        Tap tap = new DirectoryTap(new KryoScheme(Fields.UNKNOWN, fields), srcDir, SinkMode.REPLACE);
        TupleEntryCollector writer = tap.openForWrite(new LocalFlowProcess());
        
        writer.add(new Tuple("key1", 11));
        writer.add(new Tuple("key1", 12));
        writer.add(new Tuple("key2", 21));
        writer.close();

        Tap sourceTap = new DirectoryTap(new KryoScheme(fields), srcDir, SinkMode.KEEP);
        
        Pipe p = new Pipe("pipe");
        p = new Rename(p, new Fields("key"), new Fields("kee"));
        
        // Create a sink where we're writing out the fields in a different order. We also
        // test that we handle getting passed a single field when we're being used as a
        // sink (implicitly used for both source & sink).
        Tap sinkTap = new DirectoryTap(new KryoScheme(new Fields("kee", "value")), dstFile, SinkMode.REPLACE);
        Flow f = new LocalFlowConnector().connect(sourceTap, sinkTap, p);
        f.complete();
        
        // Verify we have expected output
        Tap validationTap = new DirectoryTap(new KryoScheme(new Fields("kee", "value")), dstFile);
        TupleEntryIterator iter = validationTap.openForRead(new LocalFlowProcess());
        
        assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        assertEquals("key1", te.getString("kee"));
        assertEquals(11, te.getInteger("value"));
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("key1", te.getString("kee"));
        assertEquals(12, te.getInteger("value"));
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("key2", te.getString("kee"));
        assertEquals(21, te.getInteger("value"));
        
        assertFalse(iter.hasNext());
        
        iter.close();
        
    }

}
