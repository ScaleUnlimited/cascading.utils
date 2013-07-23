package com.scaleunlimited.cascading.ml;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.mahout.math.stats.LogLikelihood;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.SumBy;
import cascading.pipe.joiner.LeftJoin;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.scaleunlimited.cascading.NullContext;

@SuppressWarnings("serial")
public class TopTermsByLLR extends SubAssembly {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopTermsByLLR.class);
    
    private static class ExtractTerms extends BaseOperation<NullContext> implements Function<NullContext> {
        
        private ITermsParser _parser;
        
        public ExtractTerms(ITermsParser parser) {
            super(new Fields("term", "term_count"));
            _parser = parser;
        }

        @SuppressWarnings("rawtypes")
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            _parser.reset(functionCall.getArguments().getString(0));
            
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
            
            functionCall.getOutputCollector().add(new Tuple(null, totalTerms));
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

    private static class TermAndCounts {
        public boolean atEnd;
        
        public String curTerm;
        public int docTermCount;
        public int totalTermCount;
        
        // Save information from next term we find.
        public String nextTerm;
        public int nextDocTermCount;
        public int nextTotalTermCount;
        
        @Override
        public String toString() {
            return String.format("\"%s\"=%d/%d, next \"%s\"=%d", curTerm, docTermCount, totalTermCount, nextTerm, nextDocTermCount);
        }
    }
    
    private static class CalcLLR extends BaseOperation<NullContext> implements Buffer<NullContext> {
        
        private ITermsFilter _filter;
        private ITermsParser _parser;
        
        public CalcLLR(ITermsParser parser, ITermsFilter filter) {
            super(new Fields("terms", "scores"));
            
            _parser = parser;
            _filter = filter;
        }

        @SuppressWarnings("rawtypes")
        @Override
        public void operate(FlowProcess flowProcess, BufferCall<NullContext> bufferCall) {
            TupleEntry docid = bufferCall.getGroup();
            
            Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
            if (!iter.hasNext()) {
                throw new RuntimeException(String.format("Impossible situation - group for docid %s has no members", docid));
            }
            
            TermAndCounts termCounts = new TermAndCounts();
            countTerms(iter, termCounts);
            
            if (termCounts.curTerm != null) {
                throw new RuntimeException(String.format("Impossible situation - first term for docid %s isn't null", docid));
            }
            
            int globalTermCount = termCounts.totalTermCount;
            int docTermCount = termCounts.docTermCount;

            // We can get multiple 
            // Now we can start iterating over the terms for this document, calculating their LLR score and keeping
            // the top N
            
            int maxResults = _filter.getMaxResults();
            List<TermAndScore> queue = new ArrayList<TermAndScore>(maxResults);

            while (countTerms(iter, termCounts)) {
                // LOGGER.info(termCounts);
                
                int termCount = termCounts.docTermCount;
                int termTotalCount = termCounts.totalTermCount;
                
                // k11 is the count of this term in this document
                long k11 = termCount;
                
                // k12 is the count of all other terms in this document
                long k12 = docTermCount - termCount;
                
                // k21 is the count of this term in all other documents.
                long k21 = termTotalCount - termCount;
                
                // k22 is the count of all other terms in all other documents
                long k22 = globalTermCount - docTermCount - k21;
                
                double score;
                
                try {
                    score = LogLikelihood.rootLogLikelihoodRatio(k11, k12, k21, k22);
                } catch (IllegalArgumentException e) {
                    LOGGER.warn(String.format("Invalid LLR values for %s in %s: k11=%d, k12=%d, k21=%d, k22=%d", 
                                    termCounts.curTerm, docid.getTuple(), k11, k12, k21, k22));
                    continue;
                }
                
                // See if any filtering is needed.
                if (_filter.filter(score, termCounts.curTerm, _parser)) {
                    continue;
                }
                
                if (queue.size() < maxResults) {
                    queue.add(new TermAndScore(termCounts.curTerm, score));
                    if (queue.size() == maxResults) {
                        // Set up for next call, where last score must be lowest
                        Collections.sort(queue);
                    }
                } else if (queue.get(maxResults - 1)._score < score) {
                    queue.set(maxResults - 1, new TermAndScore(termCounts.curTerm, score));
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
            
            bufferCall.getOutputCollector().add(new Tuple(terms, scores));
        }

        private boolean countTerms(Iterator<TupleEntry> iter, TermAndCounts termCounts) {
            if (termCounts.atEnd) {
                return false;
            }
            
            // While the iterator is returning terms equal to what's in termCounts, keep
            // counting the number of terms. Once the term changes, or we have no more
            // terms, return results.
            termCounts.curTerm = termCounts.nextTerm;
            termCounts.docTermCount = termCounts.nextDocTermCount;
            termCounts.totalTermCount = termCounts.nextTotalTermCount;
            
            termCounts.nextTerm = null;
            termCounts.nextDocTermCount = 0;
            termCounts.nextTotalTermCount = 0;
            
            String curTerm = termCounts.curTerm;
            while (iter.hasNext()) {
                TupleEntry te = iter.next();
                String newTerm = te.getString("term");
                int newTermCount = te.getInteger("term_count");
                
                if ((curTerm == null) && (newTerm == null)) {
                    // We have a match. Special case for when we're processing the special
                    // null term, since we won't have a previous item.
                    if (termCounts.totalTermCount == 0) {
                        termCounts.totalTermCount = te.getInteger("total_count");
                    }
                } else if ((curTerm == null) || !newTerm.equals(curTerm)) {
                    // Switching terms, return what we've got.
                    termCounts.nextTerm = newTerm;
                    termCounts.nextDocTermCount = newTermCount;
                    termCounts.nextTotalTermCount = te.getInteger("total_count");
                    return true;
                }
                
                termCounts.docTermCount += newTermCount;
            }
            
            // We ran out of terms, so we're done.
            termCounts.atEnd = true;
            return true;
        }
    }

    public TopTermsByLLR(Pipe docsPipe, ITermsParser parser, final int maxTerms) {
        this(docsPipe, parser, new ITermsFilter() {
            
            @Override
            public int getMaxResults() {
                return maxTerms;
            }
            
            @Override
            public boolean filter(double llrScore, String term, ITermsParser parser) {
                return false;
            }
        }, new Fields("docId"), new Fields("text"), AggregateBy.CompositeFunction.DEFAULT_THRESHOLD);
    }

    /**
     * Given a pipe containing tuples with <docIdFields> and a <textField>, first parse
     * the text using <parser>, then filter using <filter>. Score the resulting terms
     * using LLR, and only emit terms that pass <filter>.
     * 
     * @param docsPipe
     * @param parser
     * @param filter
     * @param docIdFields
     * @param textField
     * @param threshold - size of LRU cache for map-side pre-aggregation.
     */
    public TopTermsByLLR(Pipe docsPipe, ITermsParser parser, ITermsFilter filter, Fields docIdFields, Fields textField, int threshold) {
        
        // We assume each document has one or more fields that identify each "document", and a text field
        Pipe termsPipe = new Pipe("terms", docsPipe);
        termsPipe = new Each(termsPipe, textField, new ExtractTerms(parser), Fields.SWAP);
        
        // We've got docid, term, term count. Generate term, total count. This will
        // include the null term which will be the total count of all terms.
        Pipe termCountPipe = new Pipe("term count", termsPipe);
        termCountPipe = new SumBy(termCountPipe, new Fields("term"), new Fields("term_count"), 
                        new Fields("total_count"), Integer.class, threshold);
        // termCountPipe = new Each(termCountPipe, new Debug("summed", true));
        
        // Join termCountsPipe with our termsPipe by term, so we get
        // docid, term, doc term count, total term count
        // This will include docid, null, total terms in doc, global terms count
        Pipe allTermData = new CoGroup( termsPipe,  new Fields("term"), 
                                        termCountPipe, new Fields("term"),
                                        docIdFields.append(new Fields("term", "term_count", "term_ignore", "total_count")),
                                        new LeftJoin());
        
        Fields termFields = new Fields("term", "term_count", "total_count");
        allTermData = new Each(allTermData, docIdFields.append(termFields), new Identity());
        // allTermData = new Each(allTermData, new Debug("grouped", true));

        allTermData = new GroupBy(allTermData, docIdFields, new Fields("term"));
        allTermData = new Every(allTermData, termFields, new CalcLLR(parser, filter), Fields.SWAP);
        
        setTails(allTermData);
    }
}
