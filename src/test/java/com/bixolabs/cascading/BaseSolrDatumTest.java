package com.bixolabs.cascading;

import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

import cascading.tuple.Fields;

public class BaseSolrDatumTest extends Assert {

    @SuppressWarnings("serial")
    private static class MyDatum extends BaseSolrDatum {
        
        public static final String DATE_FN = BaseDatum.fieldName(MyDatum.class, "date");

        public static final Fields FIELDS = new Fields(DATE_FN);
        
        public MyDatum(long date) {
            super(FIELDS);
            
            setDate(date);
        }
        
        public void setDate(long date) {
            setDateField(DATE_FN, date);
        }
        
        public long getDate() {
            return getDateField(DATE_FN);
        }
    }
    
    @Test
    public void testRoundTrip() throws Exception {
        Date now = new Date(0);
        MyDatum datum = new MyDatum(now.getTime());
        assertEquals("1970-01-01T00:00:00.000Z", datum.getTuple().toString());
        assertEquals(0, datum.getDate());
    }

}
