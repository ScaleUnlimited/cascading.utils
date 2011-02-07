package com.bixolabs.cascading;

import cascading.tuple.TupleEntry;

public abstract class BaseSinkSplitter {

    public abstract String getSplitKey(TupleEntry tupleEntry);
}
