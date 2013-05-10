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
import cascading.operation.OperationCall;
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
import com.scaleunlimited.cascading.UniqueCount;

@SuppressWarnings("serial")
public class TopTermsByTfIdf extends SubAssembly {

    private static class ExtractTerms extends BaseOperation<NullContext> implements Function<NullContext> {
        
        private ITermsParser _parser;
        
        private transient Tuple _result;
        private transient Tuple _emptyTerm;
        
        public ExtractTerms(ITermsParser parser) {
            super(new Fields("term", "tf", "joiner"));
            _parser = parser;
        }

        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            
            _result = new Tuple("", 0.0f, "");
            _emptyTerm = new Tuple("", 0.0f, "x");
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
                if ((term == null) || term.isEmpty()) {
                    // Ignore empty terms, as that messes with our logic below.
                    continue;
                }
                
                totalTerms += 1;
                Integer termCount = terms.get(term);
                if (termCount == null) {
                    terms.put(term, 1);
                } else {
                    terms.put(term, termCount + 1);
                }
            }
            
            for (String term : terms.keySet()) {
                _result.setString(0, term);
                _result.setFloat(1, (float)terms.get(term)/(float)totalTerms);
                functionCall.getOutputCollector().add(_result);
            }
            
            // And output special empty term, that we use to get a count of total documents.
            functionCall.getOutputCollector().add(_emptyTerm);
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

    // TODO also take in IScorer scorer, which has methods to calculate TF score
    // from term count and document count, and IDF score from doc count & total docs.
    
    // TODO also take in IFilterTerms filter, which has methods to filter out
    // terms based on term info (term itself, term count in doc, total terms in
    // doc, TF score) and doc info (term itself, doc count, total docs, IDF score)
    
    // TODO take in Fields param wich has field(s) for use as docid.
    
    // TODO take in Fields param which has field for text.
    public TopTermsByTfIdf(Pipe docsPipe, ITermsParser parser, int numTerms) {
        
        // We assume each document has a docid field, and a text field
        Pipe termsPipe = new Pipe("terms", docsPipe);
        termsPipe = new Each(termsPipe, new Fields("text"), new ExtractTerms(parser), Fields.REPLACE);
        
        // We've got (docid, term, tf, "") for regular tuples, and (docid, "", 0.0f, "x") for
        // special tuples used to count the total number of documents.

        // We need term, IDF score. To get that, we need to calculate doc count for each term, and total doc count,
        // and do the division.
        Pipe docCountPipe = new Pipe("doc count", termsPipe);
        docCountPipe = new UniqueCount(docCountPipe, new Fields("term"), new Fields("docid"), new Fields("doc_count"));
        
        // In the docCountPipe, we now have (term, doc_count)
        // Generate term, total count. This will
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
