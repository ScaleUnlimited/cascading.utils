package com.scaleunlimited.cascading.ml;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.DebugLevel;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.local.InMemoryTap;

public class SimHashTest {

    @Test
    public void testSimple() throws Exception{
        Tuple[] inputData = new Tuple[] {
                        new Tuple("1", "a"),
                        new Tuple("1", "b"),
                        new Tuple("1", "c"),
                        new Tuple("2", "a"),
                        new Tuple("2", "b"),
                        new Tuple("2", "c")
        };
        
        Tuple[] results = new Tuple[] {
                        new Tuple("1", "2", 1.0f)
        };
        
        runTest(inputData, results);
    }

    @Test
    public void testProbabilities() throws Exception {
        // Just two documents, but lots of values per doc,
        // where 90% are the same.
        final int numWordsPerDoc = 10000;
        final float similarity = 0.90f;
        
        Tuple[] inputData = new Tuple[numWordsPerDoc * 2];
        int index = 0;
        Random rand = new Random(0L);
        int numSimilarWords = 0;
        for (int i = 0; i < numWordsPerDoc; i++) {
            inputData[index++] = new Tuple("1", "word-" + i);
            
            if (rand.nextFloat() < similarity) {
                numSimilarWords += 1;
                inputData[index++] = new Tuple("2", "word-" + i);
            } else {
                inputData[index++] = new Tuple("2", "otherword-" + i);
            }
        }
        
        float trueSimilarity = (float)numSimilarWords/(float)numWordsPerDoc;
        
        Tuple[] results = new Tuple[] {
                        new Tuple("1", "2", trueSimilarity)
        };
        
        final int numHashes = 100;
        runTest(inputData, results, numHashes, 1);
    }

    @Test
    public void testDupTerms() throws Exception {
        Tuple[] inputData = new Tuple[] {
                        new Tuple("1", "a"),
                        new Tuple("1", "a"),
                        new Tuple("1", "a"),
                        new Tuple("1", "b"),
                        new Tuple("1", "b"),
                        new Tuple("1", "b"),
                        new Tuple("1", "d"),
                        new Tuple("1", "d"),
                        new Tuple("1", "d"),
                        new Tuple("2", "a"),
                        new Tuple("2", "b"),
                        new Tuple("2", "c")
        };
        
        Tuple[] results = new Tuple[] {
                        new Tuple("1", "2", 0.667f)
        };
        
        runTest(inputData, results, 3, 1);
    }
    
    @Test
    public void testWordsInArticles() throws Exception {
        List<Tuple> inputData = new ArrayList<Tuple>();
        String[] articles = new String[] {
            "bosnia is the largest geographic region of the modern state with a moderate continental climate marked by hot summers and cold snowy winters",
            "the inland is a geographically larger region and has a moderate continental climate bookended by hot summers and cold and snowy winters"
        };
        
        for (int i = 0; i < articles.length; i++) {
            String article = articles[i];
            for (String word : article.split(" ")) {
                inputData.add(new Tuple("" + (i + 1), word));
            }
        }
        
        Tuple[] results = new Tuple[] {
                        new Tuple("1", "2", 0.667f)
        };
        
        runTest(inputData.toArray(new Tuple[inputData.size()]), results, 10, 1);
    }
    
    @Test
    public void testComplex() throws Exception {
        Tuple[] inputData = new Tuple[] {
                        new Tuple("1", "a"),
                        new Tuple("1", "b"),
                        new Tuple("1", "c"),
                        new Tuple("2", "a"),
                        new Tuple("2", "b"),
                        new Tuple("2", "d"),
                        new Tuple("3", "a"),
                        new Tuple("3", "e"),
                        new Tuple("3", "f")
        };
        
        Tuple[] results1 = new Tuple[] {
                        new Tuple("1", "2", 0.667f),
                        new Tuple("1", "3", 0.333f),
                        new Tuple("2", "3", 0.333f)
        };
        
        runTest(inputData, results1, 3, 2);
        
        Tuple[] results2 = new Tuple[] {
                        new Tuple("1", "2", 0.667f),
                        new Tuple("2", "3", 0.333f)
        };
        
        runTest(inputData, results2, 3, 1);
    }

    protected void runTest(Tuple[] inputTuples, Tuple[] results) throws Exception {
        runTest(inputTuples, results, 1, 1);
    }
    
    protected void runTest(Tuple[] inputTuples, Tuple[] results, int numHashes, int numSimilarDocs) throws Exception {
        final Fields sourceFields = new Fields("docId", "term");
        InMemoryTap sourceTap = new InMemoryTap(sourceFields);
        TupleEntryCollector writer = sourceTap.openForWrite(new LocalFlowProcess());
        for (Tuple inputTuple : inputTuples) {
            writer.add(inputTuple);
        }
        writer.close();
        
        Pipe p = new Pipe("source");
        p = new SimHash(p, "docId", "term", numHashes, numSimilarDocs);
        
        InMemoryTap sinkTap = new InMemoryTap(  Fields.ALL,
                                                new Fields("docId", SimHash.SIMILAR_DOC_ID_FN, SimHash.SIMILARITY_FN),
                                                SinkMode.REPLACE);
        
        FlowDef flowDef = new FlowDef()
            .setName("SimHashTest-test")
            .addSource(p, sourceTap)
            .addTailSink(p, sinkTap)
            .setDebugLevel(DebugLevel.VERBOSE);
        
        new LocalFlowConnector().connect(flowDef).complete();
        
        // Now verify we have the expected results in our result.
        assertContains(sinkTap.openForRead(new LocalFlowProcess()), results);
    }
    
    private static void assertContains(TupleEntryIterator iter, Tuple... values) {
        List<Tuple> targets = new ArrayList<Tuple>(Arrays.asList(values));
        
        while (iter.hasNext()) {
            Tuple t = iter.next().getTuple();
            boolean foundValue = false;
            
            Iterator<Tuple> targetIter = targets.iterator();
            while (targetIter.hasNext() && !foundValue) {
                Tuple targetTuple = targetIter.next();
                if (targetTuple.size() != t.size()) {
                    continue;
                }
                
                boolean matches = true;
                for (int i = 0; i < t.size() && matches; i++) {
                    Object o1 = t.getObject(i);
                    Object o2 = targetTuple.getObject(i);
                    if ((o1 instanceof Float) && (o2 instanceof Float)) {
                        // Delta is less than 5%
                        float min = (Float)o1 < (Float)o2 ? (Float)o1 : (Float)o2;
                        float max = (Float)o1 > (Float)o2 ? (Float)o1 : (Float)o2;
                        matches = ((max - min) / min) < 0.05f;
                    } else {
                        matches = t.getObject(i).equals(targetTuple.getObject(i));
                    }
                }
                
                if (matches) {
                    targetIter.remove();
                    foundValue = true;
                }
            }
            
            if (!foundValue) {
                fail("Result set had unexpected value: " + t);
            }
        }
        
        if (!targets.isEmpty()) {
            fail("Result set missing one or more expected values: " + targets.get(0));
        }
    }

}
