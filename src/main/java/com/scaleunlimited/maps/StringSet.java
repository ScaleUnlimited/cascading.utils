package com.scaleunlimited.maps;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class StringSet implements Set<String>, Writable {

    // Value returned by fastutil when we request an int that doesn't exist.
    private static final int MISSING_HASH_VALUE = -1;

    private static final int DEFAULT_ENTRY_COUNT = 1000;
    private static final int STRING_DATA_BLOCKSIZE = 64 * 1024;
    
    private Long2IntOpenHashMap _hashToOffset;
    private Set<String> _collisionSet;
    private byte[] _stringData;
    private int _curOffset;
    private boolean _smallHash; // for testing
    
    public StringSet() {
        this(false);
    }

    public StringSet(boolean smallHash) {
        reset(smallHash, DEFAULT_ENTRY_COUNT, 0, STRING_DATA_BLOCKSIZE);
    }
    
    private void reset(boolean smallHash, int numHashEntries, int numCollisionEntries, int stringDataSize) {
        _smallHash = smallHash;
        _hashToOffset = new Long2IntOpenHashMap(numHashEntries);
        _hashToOffset.defaultReturnValue(MISSING_HASH_VALUE);
        _collisionSet = new HashSet<String>(numCollisionEntries);
        _stringData = new byte[stringDataSize];
        _curOffset = 0;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        boolean smallHash = in.readBoolean();
        int numHashEntries = in.readInt();
        int numCollisionEntries = in.readInt();
        int stringDataSize = in.readInt();
        
        reset(smallHash, numHashEntries, numCollisionEntries, stringDataSize);
        
        in.readFully(_stringData, 0, stringDataSize);
        
        // Now we have to rebuild the hash table from the data in _stringData.
        for (; _curOffset < stringDataSize; ) {
            int len = calcStringLength(_curOffset);
            if (len > 0) {
                // only process strings we haven't deleted
                long hash = getLongHash(_stringData, _curOffset, len);
                int oldOffset = _hashToOffset.put(hash, _curOffset);
                if (oldOffset != MISSING_HASH_VALUE) {
                    throw new IOException("Data corruption - hash already exists!");
                }
            }
            
            // Skip over the null value.
            _curOffset += (len + 1);
        }
        
        // Now read in the collision values. For each, make sure we already have a
        // hash entry, otherwise it's an error.
        for (int i = 0; i < numCollisionEntries; i++) {
            String s = in.readUTF();
            
            long hash = getLongHash(s);
            if (!_hashToOffset.containsKey(hash)) {
                throw new IOException("Data corruption - collision entry doesn't exist in hash!");
            }
            
            if (!_collisionSet.add(s)) {
                throw new IOException("Data corruption - collision entry already exists!");
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(_smallHash);
        out.writeInt(_hashToOffset.size());
        out.writeInt(_collisionSet.size());
        out.writeInt(_curOffset);
        
        // Now just write out the string array. We can re-build the hash table from
        // this array.
        out.write(_stringData, 0, _curOffset);
        
        // Write out the strings we've saved in the collision set.
        Iterator<String> iter = _collisionSet.iterator();
        while (iter.hasNext()) {
            out.writeUTF(iter.next());
        }
    }

    private int calcStringLength(int startingOffset) {
        int curOffset = startingOffset;
        while (_stringData[curOffset] != 0) {
            curOffset += 1;
        }
        
        return curOffset - startingOffset;
    }
    
    /**
     * Generate a 64-bit JOAAT hash from the bytes of <s>
     * 
     * @param s String to hash
     * @return 64-bit hash
     */
    public long getLongHash(String s) {
        byte[] bytes = getUTF8Bytes(s);
        return getLongHash(bytes, 0, bytes.length);
    }

    /**
     * Generate a 64-bit JOAAT hash from the bytes of <s>
     * 
     * @param s String to hash
     * @return 64-bit hash
     */
    public long getLongHash(byte[] b, int offset, int length) {
        long result = 0;

        for (int i = 0; i < length; i++) {
            byte curByte = b[offset + i];
            int h = (int)curByte;
            
            result += h & 0x0FFL;
            result += (result << 20);
            result ^= (result >> 12);
        }
        
        result += (result << 6);
        result ^= (result >> 22);
        result += (result << 30);

        if (_smallHash) {
            // only generate 256 unique hash values.
            result = result & 0x0FFL;
        }
        
        return result;
    }

    /**
     * Generate a 64-bit JOAAT hash from the bytes of <phrase>
     * 
     * @param phrase String to hash
     * @return 64-bit hash
     */
    public long hash(String phrase) {
        return getLongHash(phrase);
    }
    

    @Override
    public int size() {
        return _hashToOffset.size() + _collisionSet.size();
    }

    @Override
    public boolean isEmpty() {
        return _hashToOffset.isEmpty() && _collisionSet.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        if (o instanceof String) {
            long hash = hash((String)o);
            int offset = _hashToOffset.get(hash);
            if (offset == MISSING_HASH_VALUE) {
                return false;
            }
            
            // We might have a match...need to see if the actual string matches our stored bytes.
            // If not, then we check the collision set.
            byte[] stringBytes = getUTF8Bytes((String)o);
            boolean matches = true;
            for (int i = 0; (i < stringBytes.length) && matches; i++) {
                if (stringBytes[i] != _stringData[offset + i]) {
                    matches = false;
                }
            }
            
            // If it matched all of the string bytes, make sure we've got our terminating null byte.a
            matches = matches && _stringData[offset + stringBytes.length] == 0;
            
            // If it didn't match, see if it's in the collision set.
            return(matches || _collisionSet.contains((String)o));
        } else {
            return false;
        }
    }

    private byte[] getUTF8Bytes(String str) {
        try {
            return str.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Impossible missing charset exception", e);
        }
    }
    
    @Override
    public Iterator<String> iterator() {
        throw new UnsupportedOperationException("Not yet implemented");
        // TODO Auto-generated method stub
        // We'd need to create an iterator that iterates over all of the values, and
        // also returns everything from the collision set.
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException("Not yet implemented");
        // TODO Auto-generated method stub
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException("Not yet implemented");
        // TODO Auto-generated method stub
    }

    @Override
    public boolean add(String e) {
        if (contains(e)) {
            return false;
        }
        
        long hash = hash(e);
        int offset = _hashToOffset.get(hash);
        if (offset == MISSING_HASH_VALUE) {
            // We need to add it to the array and the hash set
            byte[] stringBytes = getUTF8Bytes(e);
            
            // Make sure we have enough space in the array.
            int endOffset = _curOffset + stringBytes.length + 1;
            if (endOffset > _stringData.length) {
                byte[] newData = new byte[endOffset + STRING_DATA_BLOCKSIZE];
                System.arraycopy(_stringData, 0, newData, 0, _curOffset);
                _stringData = newData;
            }
            
            System.arraycopy(stringBytes, 0, _stringData, _curOffset, stringBytes.length);
            
            _hashToOffset.put(hash, _curOffset);
            _curOffset += stringBytes.length;
            
            // And null-terminate it.
            _stringData[_curOffset++] = 0;
        } else {
            _collisionSet.add(e);
        }

        return true;
    }

    @Override
    public boolean remove(Object o) {
        if (o instanceof String) {
            if (_collisionSet.remove(o)) {
                return true;
            } else {
                // FUTURE set up to reclaim space in string data block.
                // We'd want to save the offset somewhere
                long hash = hash((String)o);
                int stringDataOffset = _hashToOffset.remove(hash);
                if (stringDataOffset != MISSING_HASH_VALUE) {
                    // We need to clear out the entry so we don't re-add it as a string
                    // when we de-serialize things.
                    int len = calcStringLength(stringDataOffset);
                    Arrays.fill(_stringData, stringDataOffset, stringDataOffset + len, (byte)0);
                    return true;
                } else {
                    return false;
                }
            }
        } else {
            return false;
        }
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException("Not yet implemented");
        // TODO Auto-generated method stub
    }

    @Override
    public boolean addAll(Collection<? extends String> c) {
        throw new UnsupportedOperationException("Not yet implemented");
        // TODO Auto-generated method stub
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("Not yet implemented");
        // TODO Auto-generated method stub
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("Not yet implemented");
        // TODO Auto-generated method stub
    }

    @Override
    public void clear() {
        _hashToOffset.clear();
        _collisionSet.clear();
        
        // Decrease size of byte array
        if (_stringData.length > STRING_DATA_BLOCKSIZE) {
            _stringData = new byte[STRING_DATA_BLOCKSIZE];
        }
        
        _curOffset = 0;
    }

}
