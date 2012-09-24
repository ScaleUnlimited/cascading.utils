package com.scaleunlimited.cascading;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public abstract class BaseSolrDatum extends BaseDatum {

    public static final String SOLR_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private static final ThreadLocal<DateFormat> _dateFormatter = new ThreadLocal<DateFormat>() {
        
        @Override
        protected DateFormat initialValue() {
            // Use Solr date format
            SimpleDateFormat dateFormat = new SimpleDateFormat(SOLR_DATE_FORMAT);
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            return dateFormat;
        }
    };
    
    private static final ThreadLocal<Date> _date = new ThreadLocal<Date>() {
        
        @Override
        protected Date initialValue() {
            return new Date();
        }
    };

    
    public BaseSolrDatum() {
        super();
    }

    public BaseSolrDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
    }

    public BaseSolrDatum(Fields fields) {
        super(fields);
    }

    public BaseSolrDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
    }

    protected void setDateField(String fieldName, long time) {
        _tupleEntry.set(fieldName, toSolrDate(time));
    }
    
    protected long getDateField(String fieldName) {
        return fromSolrDate(_tupleEntry.getString(fieldName));
    }
    
    public static String toSolrDate(long time) {
        Date d = _date.get();
        d.setTime(time);
        return _dateFormatter.get().format(d);
    }
    
    /**
     * Convert a Solr-formatted date string back into a long time.
     * 
     * Note that this routine should be used sparingly, as it allocates a
     * new Date object with every call.
     * 
     * @param date a date formatted according to Solr's conventions
     * @return the date as a time (milliseconds since 1970)
     */
    public static long fromSolrDate(String date) {
        try {
            return _dateFormatter.get().parse(date).getTime();
        } catch (ParseException e) {
            throw new RuntimeException("Impossible exception", e);
        }
    }
    
}
