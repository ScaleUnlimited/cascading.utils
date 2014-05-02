package com.scaleunlimited.maps;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;

/**
 * A Map<String, String> that uses fastutil for native type->native type mapping, and a byte array for
 * storing the UTF-8 bytes for key/value pairs. This makes it much more efficient for storing lots of
 * small strings, and it's very fast to serialize/deserialize.
 *
 */
public class StringMap implements Map<String, String>, Writable {

    // Value returned by fastutil when we request an int that doesn't exist.
    private static final long MISSING_HASH_VALUE = -1;

    private static final int DEFAULT_ENTRY_COUNT = 1000;
    private static final int STRING_DATA_BLOCKSIZE = 64 * 1024;
    
    // FUTURE use an Int2IntOpenHashMap, with key/value in same stringData array.
    // FUTURE have multiple stringData arrays, each up to a max size, and determine which one via offset % block size.
    //        That would avoid having one gigantic block of memory that we're expanding (and copying to).
    // FUTURE track empty space in data array due to removal/put that has to move. If it gets too big relative to
    //        total file size, do a compaction. Walk data, generate up to say 10K offset/shift values (where shift
    //        keeps increasing) - move the data as we do this. Then walk the map, and do binary search into offsets,
    //        adjusting value by shift amount.
    
    private Long2LongOpenHashMap _hashToOffsets;
    private Map<String, String> _collisionMap;
    private byte[] _keyData;
    private int _curKeyOffset;
    private byte[] _valueData;
    private int _curValueOffset;
    private boolean _smallHash; // for testing
    
    public StringMap() {
        this(false);
    }

    public StringMap(boolean smallHash) {
        reset(smallHash, DEFAULT_ENTRY_COUNT, 0, STRING_DATA_BLOCKSIZE, STRING_DATA_BLOCKSIZE);
    }
    
    private void reset(boolean smallHash, int numHashEntries, int numCollisionEntries, int keyDataSize, int valueDataSize) {
        _smallHash = smallHash;
        
        _hashToOffsets = new Long2LongOpenHashMap(numHashEntries);
        _hashToOffsets.defaultReturnValue(MISSING_HASH_VALUE);
        _collisionMap = new HashMap<String, String>(numCollisionEntries);
        
        // We have separate arrays for the actual key and value string data, stored as UTF-8 bytes.
        _keyData = new byte[keyDataSize];
        _curKeyOffset = 0;
        _valueData = new byte[valueDataSize];
        _curValueOffset = 0;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        boolean smallHash = in.readBoolean();
        int numHashEntries = in.readInt();
        int numCollisionEntries = in.readInt();
        int keyDataSize = in.readInt();
        int valueDataSize = in.readInt();
        
        reset(smallHash, numHashEntries, numCollisionEntries, keyDataSize, valueDataSize);
        
        in.readFully(_keyData, 0, keyDataSize);
        in.readFully(_valueData, 0, valueDataSize);
        
        // Now we have to rebuild the hash table from the data in _keyData & _valueData.
        for (; _curKeyOffset < keyDataSize; ) {
            int keyLen = calcKeyLength(_curKeyOffset);
            if (keyLen > 0) {
                // only process strings we haven't deleted
                long hash = getLongHash(_keyData, _curKeyOffset, keyLen);
                long oldOffset = _hashToOffsets.put(hash, getOffsets(_curKeyOffset, _curValueOffset));
                if (oldOffset != MISSING_HASH_VALUE) {
                    throw new IOException("Data corruption - hash already exists!");
                }
                
                _curKeyOffset += (keyLen + 1);

                int valueLen = calcValueLength(_curValueOffset);
                _curValueOffset += (valueLen + 1);
            } else {
                // Skip removed key and removed value
                while ((_curKeyOffset < keyDataSize) && (_keyData[_curKeyOffset] == (byte)0x00)) {
                    _curKeyOffset += 1;
                }
                
                while ((_curValueOffset < valueDataSize) && (_valueData[_curValueOffset] == (byte)0x00)) {
                    _curValueOffset += 1;
                }
            }
        }
        
        // Now read in the collision values. For each, make sure we already have a
        // hash entry, otherwise it's an error.
        for (int i = 0; i < numCollisionEntries; i++) {
            String key = in.readUTF();
            String value = in.readUTF();
            
            long hash = getLongHash(key);
            if (!_hashToOffsets.containsKey(hash)) {
                throw new IOException("Data corruption - collision entry doesn't exist in hash!");
            }
            
            if (_collisionMap.put(key, value) != null) {
                throw new IOException("Data corruption - collision entry already exists!");
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(_smallHash);
        out.writeInt(_hashToOffsets.size());
        out.writeInt(_collisionMap.size());
        
        // Write out the key & value data info. We can e-build the hash table from
        // this array.
        out.writeInt(_curKeyOffset);
        out.writeInt(_curValueOffset);
        out.write(_keyData, 0, _curKeyOffset);
        out.write(_valueData, 0, _curValueOffset);

        // Write out the entries we've saved in the collision set.
        for (Entry<String, String> entry : _collisionMap.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }

    private int getKeyOffset(long offset) {
        return (int)(offset >> 32);
    }
    
    private int getValueOffset(long offset) {
        return (int)(offset & 0x0ffffffff);
    }
    
    private long getOffsets(int keyOffset, int valueOffset) {
        return ((long)keyOffset << 32) | ((long)valueOffset & 0x0ffffffff);
    }
    
    private int calcKeyLength(int startingOffset) {
        return calcStringLength(_keyData, startingOffset);
    }
    
    private int calcValueLength(int startingOffset) {
        return calcStringLength(_valueData, startingOffset);
    }
    
    private int calcStringLength(byte[] stringData, int startingOffset) {
        int curOffset = startingOffset;
        while (stringData[curOffset] != 0) {
            curOffset += 1;
        }
        
        return curOffset - startingOffset;
    }
    
    private String getValueString(int valueOffset, int valueLen) {
        try {
            return new String(_valueData, valueOffset, valueLen, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Impossible missing charset exception", e);
        }
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
        return _hashToOffsets.size() + _collisionMap.size();
    }

    @Override
    public boolean isEmpty() {
        return _hashToOffsets.isEmpty() && _collisionMap.isEmpty();
    }

    private byte[] getUTF8Bytes(String str) {
        try {
            return str.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Impossible missing charset exception", e);
        }
    }
    

    @Override
    public String remove(Object key) {
        if (key instanceof String) {
            String collisionValue = _collisionMap.remove(key);
            if (collisionValue != null) {
                return collisionValue;
            } else {
                // FUTURE set up to reclaim space in string data block.
                // We'd want to save the offset somewhere
                long hash = hash((String)key);
                long offsets = _hashToOffsets.remove(hash);
                if (offsets != MISSING_HASH_VALUE) {
                    // We need to clear out the entry so we don't re-add it as a string
                    // when we de-serialize things.
                    int keyOffset = getKeyOffset(offsets);
                    int keyLen = calcKeyLength(keyOffset);
                    Arrays.fill(_keyData, keyOffset, keyOffset + keyLen, (byte)0);
                    
                    int valueOffset = getValueOffset(offsets);
                    int valueLen = calcValueLength(valueOffset);
                    String result = getValueString(valueOffset, valueLen);
                    Arrays.fill(_valueData, valueOffset, valueOffset + valueLen, (byte)0);
                    return result;
                } else {
                    return null;
                }
            }
        } else {
            return null;
        }
    }
    
    @Override
    public void clear() {
        _hashToOffsets.clear();
        _collisionMap.clear();
        
        // Decrease size of byte arrays
        if (_keyData.length > STRING_DATA_BLOCKSIZE) {
            _keyData = new byte[STRING_DATA_BLOCKSIZE];
        }
        
        if (_valueData.length > STRING_DATA_BLOCKSIZE) {
            _valueData = new byte[STRING_DATA_BLOCKSIZE];
        }
        
        _curKeyOffset = 0;
        _curValueOffset = 0;
    }

    @Override
    public boolean containsKey(Object key) {
        if (key instanceof String) {
            long hash = hash((String)key);
            long offsets = _hashToOffsets.get(hash);
            if (offsets == MISSING_HASH_VALUE) {
                return false;
            }
            
            // We might have a match...need to see if the actual string matches our stored bytes.
            // If not, then we check the collision set.
            int keyOffset = getKeyOffset(offsets);
            byte[] stringBytes = getUTF8Bytes((String)key);
            boolean matches = true;
            for (int i = 0; (i < stringBytes.length) && matches; i++) {
                if (stringBytes[i] != _keyData[keyOffset + i]) {
                    matches = false;
                }
            }
            
            // If it matched all of the string bytes, make sure we've got our terminating null byte.
            matches = matches && _keyData[keyOffset + stringBytes.length] == 0;
            
            // If it didn't match, see if it's in the collision set.
            return(matches || _collisionMap.containsKey((String)key));
        } else {
            return false;
        }
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public String get(Object key) {
        // See if we have it in the collision map.
        String result = _collisionMap.get(key);
        if (result != null) {
            return result;
        }
        
        // TODO use common code to return either MISSING_HASH_VALUE,
        // or the long offsets value if we have the key.
        long hash = hash((String)key);
        long offsets = _hashToOffsets.get(hash);
        if (offsets == MISSING_HASH_VALUE) {
            return null;
        }

        int keyOffset = getKeyOffset(offsets);
        byte[] stringBytes = getUTF8Bytes((String)key);
        boolean matches = true;
        for (int i = 0; (i < stringBytes.length) && matches; i++) {
            if (stringBytes[i] != _keyData[keyOffset + i]) {
                matches = false;
            }
        }
        
        // If it matched all of the string bytes, make sure we've got our terminating null byte.a
        matches = matches && _keyData[keyOffset + stringBytes.length] == 0;
        
        if (matches) {
            int valueOffset = getValueOffset(offsets);
            int valueLen = calcValueLength(valueOffset);
            return getValueString(valueOffset, valueLen);
        } else {
            return _collisionMap.get(key);
        }
    }

    private boolean keyInHash(String key) {
        long hash = hash(key);
        long offsets = _hashToOffsets.get(hash);
        if (offsets == MISSING_HASH_VALUE) {
            return false;
        }

        int keyOffset = getKeyOffset(offsets);
        byte[] stringBytes = getUTF8Bytes(key);
        boolean matches = true;
        for (int i = 0; (i < stringBytes.length) && matches; i++) {
            if (stringBytes[i] != _keyData[keyOffset + i]) {
                matches = false;
            }
        }
        
        // If it matched all of the string bytes, make sure we've got our terminating null byte.a
        matches = matches && _keyData[keyOffset + stringBytes.length] == 0;
        return matches;
    }
    
    @Override
    public String put(String key, String value) {
        long hash = hash(key);
        long offsets = _hashToOffsets.get(hash);
        if (offsets == MISSING_HASH_VALUE) {
            // We need to add it to the array and the hash set
            byte[] keyBytes = getUTF8Bytes(key);
            
            // Make sure we have enough space in the array.
            int endOffset = _curKeyOffset + keyBytes.length + 1;
            if (endOffset > _keyData.length) {
                byte[] newData = new byte[endOffset + STRING_DATA_BLOCKSIZE];
                System.arraycopy(_keyData, 0, newData, 0, _curKeyOffset);
                _keyData = newData;
            }
            
            System.arraycopy(keyBytes, 0, _keyData, _curKeyOffset, keyBytes.length);
            
            byte[] valueBytes = getUTF8Bytes(value);
            endOffset = _curValueOffset + valueBytes.length + 1;
            if (endOffset > _valueData.length) {
                byte[] newData = new byte[endOffset + STRING_DATA_BLOCKSIZE];
                System.arraycopy(_valueData, 0, newData, 0, _curValueOffset);
                _valueData = newData;
            }
            
            System.arraycopy(valueBytes, 0, _valueData, _curValueOffset, valueBytes.length);

            _hashToOffsets.put(hash, getOffsets(_curKeyOffset, _curValueOffset));
            _curKeyOffset += keyBytes.length;
            _curValueOffset += valueBytes.length;
            
            // And null-terminate it.
            _keyData[_curKeyOffset++] = 0;
            _valueData[_curValueOffset++] = 0;
            
            // There was no previous value.
            return null;
        } else if (keyInHash(key)) {
            // We're updating something that's in our hash. For now, just remove it and re-add it.
            // FUTURE if new value length <= old value length, insert in-place and zero out the
            // remaining data.
            String result = remove(key);
            put(key, value);
            return result;
        } else {
            // We're adding a collision entry, or updating one that already exists.
            return _collisionMap.put(key, value);
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Set<String> keySet() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Collection<String> values() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Set<java.util.Map.Entry<String, String>> entrySet() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

}
