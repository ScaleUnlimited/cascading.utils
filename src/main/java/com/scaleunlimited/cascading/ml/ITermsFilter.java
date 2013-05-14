package com.scaleunlimited.cascading.ml;

public interface ITermsFilter {

    public boolean filter(float llrScore, String term, ITermsParser parser);
    
    public int getMaxResults();
}
