package com.scaleunlimited.cascading.ml;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.operation.aggregator.First;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Unique;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * We get passed a Tuple that has two two fields in it - a document id, and a "terms" string.
 * We'll output the the top N similar documents (with similarity scores) for each unique doc ID.
 *
 */
@SuppressWarnings("serial")
public class SimHash extends SubAssembly {

    public static final String SIMILAR_DOC_ID_FN = "SimHash_similarDocId";
    public static final Fields SIMILAR_DOC_ID_FIELD = new Fields(SIMILAR_DOC_ID_FN);

    public static final String SIMILARITY_FN = "SimHash_similarity";
    public static final Fields SIMILARITY_FIELD = new Fields(SIMILARITY_FN);
    
    private static final String TERM_HASH_FN = "SimHash_termHash";
    private static final Fields TERM_HASH_FIELD = new Fields(TERM_HASH_FN);
    
    private static final Fields NUM_SIMILAR_DOCS_FIELD = new Fields("SimHash_numSimilarDocs");
    
    private static class EmitMatchingDocs extends BaseOperation<Void> implements Buffer<Void> {
        
        private String _docIdFieldname;
        private transient Tuple _result;
        
        public EmitMatchingDocs(String docIdFieldname) {
            super(new Fields(docIdFieldname, SIMILAR_DOC_ID_FN));
            
            _docIdFieldname = docIdFieldname;
        }

        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<Void> operationCall) {
            super.prepare(flowProcess, operationCall);
            
            _result = Tuple.size(2);
        }
        
        @Override
        public void operate(FlowProcess flowProcess, BufferCall<Void> bufferCall) {
            // We're grouped on hash, sorted by doc id. So each group will have
            // some array of doc ids, where we need to emit all pairs, but we only need to
            // emit one of each; (A, B) only, not (B, A) as well.
            
            Iterator<TupleEntry> iter = bufferCall.getArgumentsIterator();
            List<Object> pendingDocIds = new ArrayList<Object>();
            
            while (iter.hasNext()) {
                TupleEntry te = iter.next();
                Object nextDocId = te.getObject(_docIdFieldname);
                _result.set(1, nextDocId);
                
                for (Object docId : pendingDocIds) {
                    _result.set(0, docId);
                    bufferCall.getOutputCollector().add(_result);
                }
                
                pendingDocIds.add(nextDocId);
            }
        }
    }
    
    private static byte[] getUTF8Bytes(String str) {
        try {
            return str.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Impossible missing charset exception", e);
        }
    }
    
    /**
     * Generate a 64-bit JOAAT hash from the bytes of <s>
     * 
     * @param s String to hash
     * @return 64-bit hash
     */
    private static long getLongHash(String s) {
        byte[] bytes = getUTF8Bytes(s);
        return getLongHash(bytes, 0, bytes.length);
    }

    /**
     * Generate a 64-bit JOAAT hash from the given byte array
     * 
     * @param b Bytes to hash
     * @param offset starting offset
     * @param length number of bytes to hash
     * @return 64-bit hash
     */
    private static long getLongHash(byte[] b, int offset, int length) {
        long result = 0;

        for (int i = 0; i < length; i++) {
            byte curByte = b[offset + i];
            int h = (int)curByte;
            
            result += h & 0x0FFL;
            result += (result << 20);
            result ^= (result >> 12);
        }
        
        result += (result << 6);
        result ^= (result >> 22);
        result += (result << 30);

        return result;
    }

    private static class CalcHash extends BaseOperation<Void> implements Function<Void> {

        private transient Tuple _result;
        
        public CalcHash() {
            super(TERM_HASH_FIELD);
        }
        
        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<Void> operationCall) {
            super.prepare(flowProcess, operationCall);
            _result = Tuple.size(1);
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall) {
            Object term = functionCall.getArguments().getObject(0);
            long hash = 0;
            
            if (term instanceof String) {
                hash = getLongHash((String)term);
            } else {
                hash = term.hashCode();
            }
            
            _result.setLong(0, hash);
            functionCall.getOutputCollector().add(_result);
        }
        
    }
    
    
    public SimHash(Pipe sourcePipe, String docIdFieldname, String termFieldname, int numHashes, int numSimilarDocs) {
        super(sourcePipe);
        
        // Calculate hash for each tuple. First leave one unique value per document.
        // FUTURE we could defer this until a custom Buffer instead of First(numHashes), to avoid
        // an extra job in the workflow.
        sourcePipe = new Unique(sourcePipe, new Fields(docIdFieldname, termFieldname));

        // sourcePipe = new Each(sourcePipe, new Fields(termFieldname), new ExpressionFunction(TERM_HASH_FIELD, "$0.hashCode()", String.class), Fields.SWAP);
        sourcePipe = new Each(sourcePipe, DebugLevel.VERBOSE, new Debug("terms", true));
        sourcePipe = new Each(sourcePipe, new Fields(termFieldname), new CalcHash(), Fields.SWAP);
        sourcePipe = new Each(sourcePipe, DebugLevel.VERBOSE, new Debug("raw hashes", true));

        // Group by doc, sort by hash, pick the first numHashes
        sourcePipe = new GroupBy("Pick min hashes", sourcePipe, new Fields(docIdFieldname), TERM_HASH_FIELD);
        // TODO want to fail if we don't get at least numHashes per doc
        sourcePipe = new Every(sourcePipe, new First(numHashes), Fields.RESULTS);
        sourcePipe = new Each(sourcePipe, DebugLevel.VERBOSE, new Debug("min hashes", true));
        
        // Group by hash, sort by doc, emit matches
        sourcePipe = new GroupBy("Emit matching docs", sourcePipe, TERM_HASH_FIELD, new Fields(docIdFieldname));
        sourcePipe = new Every(sourcePipe, new EmitMatchingDocs(docIdFieldname), Fields.RESULTS);
        sourcePipe = new Each(sourcePipe, DebugLevel.VERBOSE, new Debug("matching docs", true));

        // Group by doc id and "matching" doc id, count occurrences
        sourcePipe = new CountBy("Count matching docs", sourcePipe, new Fields(docIdFieldname, SIMILAR_DOC_ID_FN), NUM_SIMILAR_DOCS_FIELD);
        sourcePipe = new Each(sourcePipe, DebugLevel.VERBOSE, new Debug("matching doc count", true));
        
        // Calculate similarity score.
        sourcePipe = new Each(sourcePipe, NUM_SIMILAR_DOCS_FIELD, new ExpressionFunction(SIMILARITY_FIELD, String.format("$0/%s", numHashes), Float.class), Fields.SWAP);
        sourcePipe = new Each(sourcePipe, DebugLevel.VERBOSE, new Debug("SimHash results", true));

        // Limit to top numSimilarDocs by score
        sourcePipe = new GroupBy("Emit top matches", sourcePipe, new Fields(docIdFieldname), SIMILARITY_FIELD, true);
        sourcePipe = new Every(sourcePipe, new First(numSimilarDocs), Fields.RESULTS);
        sourcePipe = new Each(sourcePipe, DebugLevel.VERBOSE, new Debug("SimHash top results", true));

        setTails(sourcePipe);
    }
}
