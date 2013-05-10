package com.scaleunlimited.cascading.ml;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.mahout.math.stats.LogLikelihood;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.SumBy;
import cascading.pipe.joiner.LeftJoin;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.scaleunlimited.cascading.NullContext;

@SuppressWarnings("serial")
public class TopTermsByLLR extends SubAssembly {

    private static class ExtractTerms extends BaseOperation<NullContext> implements Function<NullContext> {
        
        private ITermsParser _parser;
        
        public ExtractTerms(ITermsParser parser) {
            super(new Fields("term", "term_count"));
            _parser = parser;
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            _parser.reset(functionCall.getArguments().getString("text"));
            
            // TODO use fastutils code, e.g. Object2IntOpenHashMap. Or get long hash from
            // string, but then we'd need to re-join top terms (by hash) against the actual
            // term at the end of the workflow
            Map<String, Integer> terms = new HashMap<String, Integer>();
            
            int totalTerms = 0;
            for (String term : _parser) {
                totalTerms += 1;
                Integer termCount = terms.get(term);
                if (termCount == null) {
                    terms.put(term, 1);
                } else {
                    terms.put(term, termCount + 1);
                }
            }
            
            for (String term : terms.keySet()) {
                functionCall.getOutputCollector().add(new Tuple(term, terms.get(term)));
            }
            
            functionCall.getOutputCollector().add(new Tuple("", totalTerms));
        }
    }

    private static class TermAndScore implements Comparable<TermAndScore> {
        String _term;
        double _score;
        
        public TermAndScore(String term, double score) {
            _term = term;
            _score = score;
        }

        @Override
        public int compareTo(TermAndScore o) {
            if (_score > o._score) {
                return -1;
            } else if (_score < o._score) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    private static class CalcLLR extends BaseOperation<NullContext> implements Buffer<NullContext> {
        
        private int _numTerms;
        
        public CalcLLR(int numTerms) {
            super(new Fields("docid", "terms", "scores"));
            _numTerms = numTerms;
        }

        @Override
        public void operate(FlowProcess flowProcess, BufferCall<NullContext> bufferCall) {
            String docid = bufferCall.getGroup().getString("docid");
            
            Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
            if (!iter.hasNext()) {
                throw new RuntimeException(String.format("Impossible situation - group for docid %s has no members", docid));
            }
            
            TupleEntry te = iter.next();
            String term = te.getString("term");
            if (!term.isEmpty()) {
                throw new RuntimeException(String.format("Impossible situation - first term for docid %s isn't empty", docid));
            }
            
            int docTermCount = te.getInteger("term_count");
            int globalTermCount = te.getInteger("total_count");
            
            // Now we can start iterating over the terms for this document, calculating their LLR score and keeping
            // the top N
            
            List<TermAndScore> queue = new ArrayList<TermAndScore>(_numTerms);

            while (iter.hasNext()) {
                te = iter.next();
                int termCount = te.getInteger("term_count");
                int termTotalCount = te.getInteger("total_terms");
                
                // k11 is the count of this term in this document
                long k11 = termCount;
                
                // k12 is the count of all other terms in this document
                long k12 = docTermCount - termCount;
                
                // k21 is the count of this term in all other documents.
                long k21 = termTotalCount - termCount;
                
                // k22 is the count of all other terms in all other documents
                // TODO KKr - should this then be subtracting k21 here, not docTermCount?
                // And also k12?
                long k22 = globalTermCount - docTermCount;
                
                double score = LogLikelihood.logLikelihoodRatio(k11, k12, k21, k22);
                if (queue.size() < _numTerms) {
                    queue.add(new TermAndScore(te.getString("term"), score));
                    Collections.sort(queue);
                } else if (queue.get(_numTerms - 1)._score < score) {
                    queue.add(new TermAndScore(te.getString("term"), score));
                    Collections.sort(queue);
                }
            }
            
            // At the end we'll have the top terms & scores
            Tuple terms = new Tuple();
            Tuple scores = new Tuple();
            
            for (TermAndScore tas : queue) {
                terms.add(tas._term);
                scores.add(tas._score);
            }
            
            bufferCall.getOutputCollector().add(new Tuple(docid, terms, scores));
        }
    }

    public TopTermsByLLR(Pipe docsPipe, ITermsParser parser, int numTerms) {
        
        // We assume each document has a docid field, and a text field
        Pipe termsPipe = new Pipe("terms", docsPipe);
        termsPipe = new Each(termsPipe, new Fields("text"), new ExtractTerms(parser), Fields.REPLACE);
        
        // We've got docid, term, term count. Generate term, total count. This will
        // include the empty term "" which will be the total count of all terms.
        Pipe termCountPipe = new Pipe("term count", termsPipe);
        termCountPipe = new SumBy(termCountPipe, new Fields("term"), new Fields("count"), new Fields("total_count"), Integer.class);
        
        // Join termCountsPipe with our termsPipe by term, so we get
        // docid, term, doc term count, total term count
        // This will include docid, "", total terms in doc, global terms count
        
        Pipe allTermData = new CoGroup( termsPipe,  new Fields("term"), 
                                        termCountPipe, new Fields("term"),
                                        new Fields("docid", "term", "term_count", "term_ignore", "total_count"),
                                        new LeftJoin());
        
        allTermData = new Each(allTermData, new Fields("docid", "term", "term_count", "total_count"), new Identity());
        allTermData = new GroupBy(allTermData, new Fields("docid"), new Fields("term"));
        allTermData = new Every(allTermData, new CalcLLR(numTerms));
        
        setTails(allTermData);
    }
}
