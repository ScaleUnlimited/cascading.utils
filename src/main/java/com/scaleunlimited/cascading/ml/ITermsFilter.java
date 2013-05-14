package com.scaleunlimited.cascading.ml;

public interface ITermsFilter {

    public boolean filter(double llrScore, String term, ITermsParser parser);
    
    public int getMaxResults();
}
