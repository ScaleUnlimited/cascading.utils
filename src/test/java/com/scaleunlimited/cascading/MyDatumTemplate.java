package com.scaleunlimited.cascading;

import java.util.Date;

import cascading.tuple.Tuple;

/**
 * Class used to test DatumCompiler. Must be top-level, not static inner class,
 * as otherwise the generated datum class isn't of the right type.
 *
 */
public class MyDatumTemplate {

    private String _name;
    private int ageAndRisk;
    private Date _date;
    private Tuple _aliases;

}
