package com.scaleunlimited.cascading.ml;

public interface ITermsParser extends Iterable<String> {

    public void reset(String text);
    
    public int getNumWords(String term);
}
