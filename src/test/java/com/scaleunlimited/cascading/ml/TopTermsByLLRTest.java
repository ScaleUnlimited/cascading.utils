package com.scaleunlimited.cascading.ml;

import java.util.ArrayList;
import java.util.Iterator;

import junit.framework.Assert;

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
        private boolean _shingle;
        
        public SplitterParser(boolean shingle) {
            _shingle = shingle;
        }
        
        @Override
        public Iterator<String> iterator() {
            String[] words = _text.split(" ");
            ArrayList<String> terms = new ArrayList<String>(words.length * 2);
            
            for (int i = 0; i < words.length; i++) {
                terms.add(words[i]);
                if (_shingle && (i + 1 < words.length)) {
                    terms.add(words[i] + " " + words[i + 1]);
                }
            }
            
            return terms.iterator();
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
    
    private static class TestFilter implements ITermsFilter {

        @Override
        public boolean filter(double llrScore, String term, ITermsParser parser) {
            return llrScore < 1.0;
        }

        @Override
        public int getMaxResults() {
            return 20;
        }
        
    }
    
    @Test
    public void testLlrScores() throws Exception {
        InMemoryTap sourceTap = new InMemoryTap(new Fields("docId", "text"));
        TupleEntryCollector writer = sourceTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("1", "aaa xxx"));
        writer.add(new Tuple("1", "xxx"));
        writer.add(new Tuple("2", "xxx bbb"));
        writer.close();
        
        Pipe p = new Pipe("docs");
        SubAssembly ttbllr = new TopTermsByLLR(p, new SplitterParser(false), 1);
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

    @Test
    public void testFiltering() throws Exception {
        Fields groupFields = new Fields("docnum1", "docnum2");
        InMemoryTap sourceTap = new InMemoryTap(groupFields.append(new Fields("content")));
        TupleEntryCollector writer = sourceTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("1", "a", "aaa xxx xxx"));
        writer.add(new Tuple("1", "a", "aaa xxx"));
        writer.add(new Tuple("2", "b", "xxx xxx bbb bbb"));
        writer.close();
        
        Pipe p = new Pipe("docs");
        SubAssembly ttbllr = new TopTermsByLLR( p, 
                                                new SplitterParser(true), 
                                                new TestFilter(), 
                                                groupFields, 
                                                new Fields("content"),
                                                100);
        Pipe results = new Pipe("scores", ttbllr.getTails()[0]);
        results = new GroupBy(results, groupFields);
        results = new Each(results, new Debug("scored", true));
        
        Fields resultFields = groupFields.append(new Fields("terms", "scores"));
        InMemoryTap sinkTap = new InMemoryTap(resultFields, resultFields, SinkMode.REPLACE);
        
        Flow f = new LocalFlowConnector().connect(sourceTap, sinkTap, results);
        f.complete();
        
        TupleEntryIterator iter = sinkTap.openForRead(new LocalFlowProcess());
        
        // We should have one entry for doc "1", and one entry for doc "2". Each entry
        // should have a single entry
        assertTrue(iter.hasNext());
        TupleEntry te = iter.next();
        assertEquals("1", te.getString("docnum1"));
        assertEquals("a", te.getString("docnum2"));
        Tuple terms = (Tuple)te.getObject("terms");
        assertNotNull(terms);
        assertEquals(2, terms.size());
        assertEquals("aaa", terms.getString(0));
        assertEquals("aaa xxx", terms.getString(1));
        
        // The score for the term "aaa" in document 1 should be based on
        // k11 = 2 (count of term in doc 1)
        // k12 = 6 (count of other terms in doc 1)
        // k21 = 0 (count of term in other docs)
        // k22 = 7 (count of other terms in other docs)
        Tuple scores = (Tuple)te.getObject("scores");
        assertEquals(2, scores.size());
        double score = LogLikelihood.rootLogLikelihoodRatio(2, 6, 0, 7);
        assertEquals(score, scores.getDouble(0), .0001);
        
        assertTrue(iter.hasNext());
        te = iter.next();
        assertEquals("2", te.getString("docnum1"));
        assertEquals("b", te.getString("docnum2"));
        terms = (Tuple)te.getObject("terms");
        assertNotNull(terms);
        assertEquals(3, terms.size());
        assertEquals("bbb", terms.getString(0));
        assertEquals("bbb bbb", terms.getString(1));
        assertEquals("xxx bbb", terms.getString(2));
        
        // The score for the term "bbb" in document 2 should be based on
        // k11 = 2 (count of term in doc 2)
        // k12 = 5 (count of other terms in doc 2)
        // k21 = 0 (count of term in other docs)
        // k22 = 8 (count of other terms in other docs)
        scores = (Tuple)te.getObject("scores");
        assertEquals(3, scores.size());
        score = LogLikelihood.rootLogLikelihoodRatio(2, 5, 0, 8);
        assertEquals(score, scores.getDouble(0), .0001);
        
        assertFalse(iter.hasNext());
        iter.close();
    }

}
