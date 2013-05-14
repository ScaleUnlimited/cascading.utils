package com.scaleunlimited.cascading.ml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.mahout.math.stats.LogLikelihood;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.local.InMemoryTap;

public class TopTermsByLLRTest extends Assert {

    private static class SplitterParser implements ITermsParser {

        private String _text;
        
        @Override
        public Iterator<String> iterator() {
            return new ArrayList<String>(Arrays.asList(_text.split(" "))).iterator();
        }

        @Override
        public void reset(String text) {
            _text = text;
        }

        @Override
        public int getNumWords(String term) {
            return 1;
        }
    }
    
    
    @Test
    public void test() throws Exception {
        InMemoryTap sourceTap = new InMemoryTap(new Fields("docId", "text"));
        TupleEntryCollector writer = sourceTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("1", "aaa xxx"));
        writer.add(new Tuple("1", "xxx"));
        writer.add(new Tuple("2", "xxx bbb"));
        writer.close();
        
        Pipe p = new Pipe("docs");
        SubAssembly ttbllr = new TopTermsByLLR(p, new SplitterParser(), 1);
        Pipe results = new Pipe("scores", ttbllr.getTails()[0]);
        results = new GroupBy(results, new Fields("docId"));
        results = new Each(results, new Debug("scored", true));
        
        Fields resultFields = new Fields("docId", "terms", "scores");
        InMemoryTap sinkTap = new InMemoryTap(resultFields, resultFields, SinkMode.REPLACE);
        
        Flow f = new LocalFlowConnector().connect(sourceTap, sinkTap, results);
        f.complete();
        
        TupleEntryIterator iter = sinkTap.openForRead(new LocalFlowProcess());
        
        // We should have one entry for doc "1", and one entry for doc "2". Each entry
        // should have a single entry
        assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        assertEquals("1", te.getString("docId"));
        Tuple terms = (Tuple)te.getObject("terms");
        assertNotNull(terms);
        assertEquals(1, terms.size());
        assertEquals("aaa", terms.getString(0));
        
        // The score for the term "aaa" in document 1 should be based on
        // k11 = 1 (count of term in doc 1)
        // k12 = 2 (count of other terms in doc 1)
        // k21 = 0 (count of term in other docs)
        // k22 = 2 (count of other terms in other docs)
        Tuple scores = (Tuple)te.getObject("scores");
        assertEquals(1, scores.size());
        double score = LogLikelihood.rootLogLikelihoodRatio(1, 2, 0, 2);
        assertEquals(score, scores.getDouble(0), .0001);
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("2", te.getString("docId"));
        terms = (Tuple)te.getObject("terms");
        assertNotNull(terms);
        assertEquals(1, terms.size());
        assertEquals("bbb", terms.getString(0));
        
        // The score for the term "bbb" in document 2 should be based on
        // k11 = 1 (count of term in doc 2)
        // k12 = 1 (count of other terms in doc 2)
        // k21 = 0 (count of term in other docs)
        // k22 = 3 (count of other terms in other docs)
        scores = (Tuple)te.getObject("scores");
        assertEquals(1, scores.size());
        score = LogLikelihood.rootLogLikelihoodRatio(1, 1, 0, 3);
        assertEquals(score, scores.getDouble(0), .0001);
        
        assertFalse(iter.hasNext());
        iter.close();
    }

}
