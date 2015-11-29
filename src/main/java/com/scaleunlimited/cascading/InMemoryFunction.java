package com.scaleunlimited.cascading;

import cascading.operation.Function;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.Tap;

public class InMemoryFunction extends SubAssembly {

    /**
     * Apply a function to every Tuple in a pipe, where the function will be reading
     * into memory data provided by the sideData tap(s). We set up a dependency on
     * sideData such that Cascading will defer running this function until after the
     * data has been created.
     * 
     * This subassembly sets up a special SourceTap that acts like a MultiSourceTap
     * for sideData, but returns no data. It gets Merged with the provided pipe, which
     * is how a Cascade "knows" that this function (and thus this part of the Flow)
     * can't be run until the upstream Flow that creates this sideData has completed.
     * 
     * We assume that the function has been constructed with sideData, and that the
     * prepare() method will load the required data into memory.
     * 
     * @param p
     * @param function
     * @param sideData
     */
    public InMemoryFunction(Pipe p, Function<?> function, Tap<?, ?, ?>... sideData) {
        
    }
}
