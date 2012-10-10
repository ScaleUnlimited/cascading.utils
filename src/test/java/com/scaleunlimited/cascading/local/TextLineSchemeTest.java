package com.scaleunlimited.cascading.local;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

import cascading.flow.local.LocalFlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

public class TextLineSchemeTest {

    @Test
    public void testCompressedInput() throws Exception {
        Tap unpackedTap = new FileTap(new TextLineScheme(), "src/test/resources/sample-text.txt", SinkMode.KEEP);
        Tap packedTap = new FileTap(new TextLineScheme(), "src/test/resources/sample-text.txt.gz", SinkMode.KEEP);
        
        TupleEntryIterator unpackedIter = unpackedTap.openForRead(new LocalFlowProcess());
        TupleEntryIterator packedIter = packedTap.openForRead(new LocalFlowProcess());
        
        while (unpackedIter.hasNext()) {
            assertTrue(packedIter.hasNext());
            assertEquals(unpackedIter.next(), packedIter.next());
        }
        
        assertFalse(packedIter.hasNext());
    }

    @Test
    public void testCompressedOutput() throws Exception {
        // We only want the line of text.
        Tap unpackedTap = new FileTap(new TextLineScheme(new Fields("line")), "src/test/resources/sample-text.txt", SinkMode.KEEP);
        
        final String outputDir = "build/test/TextLineSchemeTest/";
        new File(outputDir).mkdirs();
        final File outputFile = new File(outputDir + "sample-text-txt.gz");
        outputFile.delete();
        
        Tap resultTap = new FileTap(new TextLineScheme(true), outputFile.getAbsolutePath(), SinkMode.REPLACE);
        TupleEntryIterator unpackedIter = unpackedTap.openForRead(new LocalFlowProcess());
        TupleEntryCollector writer = resultTap.openForWrite(new LocalFlowProcess());
        
        while (unpackedIter.hasNext()) {
            writer.add(unpackedIter.next());
        }
        writer.close();
        unpackedIter.close();
        
        // Now verify that what we wrote matches what we're expecting.
        
        Tap packedTap = new FileTap(new TextLineScheme(), "src/test/resources/sample-text.txt.gz", SinkMode.KEEP);
        TupleEntryIterator packedIter = packedTap.openForRead(new LocalFlowProcess());
        TupleEntryIterator resultIter = resultTap.openForRead(new LocalFlowProcess());
        
        while (packedIter.hasNext()) {
            assertTrue(resultIter.hasNext());
            assertEquals(packedIter.next(), resultIter.next());
        }
        
        assertFalse(resultIter.hasNext());

    }

}
