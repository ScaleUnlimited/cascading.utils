package com.bixolabs.cascading.local;

import static org.junit.Assert.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
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
    public void testInWorkflow() throws Exception {
        final String srcDir = "build/test/KryoSchemeTest/testInWorkflow/src";
        final String destDir = "build/test/KryoSchemeTest/testInWorkflow/dst";
        
        // Create a local tap that uses the KryoScheme
        Fields fields = new Fields("key", "value");
        
        Tap srcTap = new FileTap(new KryoScheme(fields), srcDir);
        TupleEntryCollector writer = srcTap.openForWrite(new LocalFlowProcess());
        
        writer.add(new Tuple("key1", 11));
        writer.add(new Tuple("key1", 12));
        writer.add(new Tuple("key2", 21));
        writer.close();

        Pipe p = new Pipe("pipe");
        p = new SumBy(p, new Fields("key"), new Fields("value"), new Fields("sum"), Integer.class);
        
        Tap sinkTap = new FileTap(new TextLine(), destDir, SinkMode.REPLACE);
        Flow f = new LocalFlowConnector().connect(srcTap, sinkTap, p);
        f.complete();
        
        // TODO verify we have expected output
        Tap validationTap = new FileTap(new TextDelimited(new Fields("key", "sum"), "\t", new Class[] {String.class, Integer.class}), destDir);
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

}
