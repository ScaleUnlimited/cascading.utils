package com.scaleunlimited.cascading;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// TODO Add test that verifies grouping/sorting works properly when using
// this as a group/sort field.
public class UUIDWritable implements WritableComparable<UUIDWritable> {
    
    private long _hiBits;
    private long _loBits;
    
    public UUIDWritable() {
        // For de-serialization
    }
    
    public UUIDWritable(UUID uuid) {
        _hiBits = uuid.getMostSignificantBits();
        _loBits = uuid.getLeastSignificantBits();
    }

    public UUID getUUID() {
        return new UUID(_hiBits, _loBits);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(_hiBits);
        out.writeLong(_loBits);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        _hiBits = in.readLong();
        _loBits = in.readLong();
    }

    @Override
    public int compareTo(UUIDWritable o) {
        if (_hiBits < o._hiBits) {
            return -1;
        } else if (_hiBits > o._hiBits) {
            return 1;
        } else if (_loBits < o._loBits) {
            return -1;
        } else if (_loBits > o._loBits){
            return 1;
        } else {
            return 0;
        }
    }
    
    
    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (_hiBits ^ (_hiBits >>> 32));
        result = prime * result + (int) (_loBits ^ (_loBits >>> 32));
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        UUIDWritable other = (UUIDWritable) obj;
        if (_hiBits != other._hiBits)
            return false;
        if (_loBits != other._loBits)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return getUUID().toString();
    }

    // A Comparator that compares serialized UUIDWritable.
    public static class Comparator extends WritableComparator {
        
        public Comparator() {
            super(UUIDWritable.class);
        }

        public int compare( byte[] b1, int s1, int l1,
                            byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    static {
        // register this comparator
        WritableComparator.define(UUIDWritable.class, new Comparator());
    }
    
}