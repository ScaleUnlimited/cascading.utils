package com.scaleunlimited.cascading;

import cascading.operation.Identity;
import cascading.operation.state.Counter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.SumBy;
import cascading.pipe.assembly.Unique;
import cascading.tuple.Fields;

/**
 * Cascading sub-assembly that generates TF*IDF values for every unique term.
 * 
 * The <termsPipe> passed to the constructor must contain tuples with the following fields:
 * 
 *  - a "doc" field, which is a string with a document identifier.
 *  - a "term" field, which is a string.
 *  - a "termcount" field, which is an integer.
 *  
 * The output is a pipe that contains tuples with the following fields:
 * 
 *  - a "term" field, which is a string
 *  - a "tf-idf" field, which is a float.
 *
 */
@SuppressWarnings("serial")
public class TfIdfAssembly extends SubAssembly {

    public static final String DOC_FN = "doc";
    public static final String TERM_FN = "term";
    public static final String TERM_COUNT_FN = "termcount";
    public static final String TF_IDF_FN = "tf-idf";
    
    private static enum Counters {
        TOTAL_DOCS
    }
    
    public TfIdfAssembly(Pipe termsPipe) {
        
        // For each term, we need to get a per-document count.
        Pipe termPerDocCountPipe = new Pipe("term count per doc pipe", termsPipe);
        termPerDocCountPipe = new SumBy(termPerDocCountPipe, 
                                        new Fields("TERM_FN"), 
                                        new Fields(TERM_COUNT_FN), 
                                        new Fields("term-per-doc-count"), 
                                        Integer.class);
        
        // We need the count of # of documents that contain each term.
        Pipe termDocCountPipe = new Pipe("term doc count pipe", termsPipe);
        termDocCountPipe = new Each(termDocCountPipe, new Fields(DOC_FN, TERM_FN), new Identity());
        termDocCountPipe = new Unique(termDocCountPipe, new Fields(DOC_FN, TERM_FN));

        // We'll also need a total document count - let's split this off the pipe after we've
        // got unique doc/term pairs, as that will have less data.
        Pipe docCountPipe = new Pipe("total doc count pipe", termDocCountPipe);
        docCountPipe = new Each(docCountPipe, new Fields(DOC_FN), new Identity());
        docCountPipe = new Unique(docCountPipe, new Fields(DOC_FN));
        docCountPipe = new Each(docCountPipe, new Counter(Counters.TOTAL_DOCS));
        // TODO add custom function that gets counter value, writes to HDFS?
        
        // Now continue with figuring out the document count for each term.
        termDocCountPipe = new CountBy(termDocCountPipe, new Fields(TERM_FN), new Fields("term-doc-count"));

    }
}
