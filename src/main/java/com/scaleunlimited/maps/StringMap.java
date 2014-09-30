package com.scaleunlimited.maps;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

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
    private static final int MISSING_HASH_VALUE = -1;

    private static final int DEFAULT_ENTRY_COUNT = 1000;
    private static final int STRING_DATA_BLOCKSIZE = 64 * 1024;
    
    // FUTURE have multiple stringData arrays, each up to a max size, and determine which one via offset % block size.
    //        That would avoid having one gigantic block of memory that we're expanding (and copying to).
    // FUTURE do in-place put if new key/value fit where old key/value was located.
    // FUTURE make it more efficient by skipping conversion of string to byte array, unless the key contains a
    //        character > 0x7F (which means it's not something that fits in one byte in UTF-8)
    // FUTURE track empty space in data array due to removal/put that has to move. If it gets too big relative to
    //        total file size, do a compaction. Walk data, generate up to say 10K offset/shift values (where shift
    //        keeps increasing) - move the data as we do this. Then walk the map, and do binary search into offsets,
    //        adjusting value by shift amount.
    
    private Int2IntOpenHashMap _hashToOffsets;
    private Map<String, String> _collisionMap;
    private byte[] _stringData;
    private int _curStringOffset;
    private boolean _smallHash; // for testing
    
    public StringMap() {
        this(false);
    }

    public StringMap(boolean smallHash) {
        reset(smallHash, DEFAULT_ENTRY_COUNT, 0, STRING_DATA_BLOCKSIZE);
    }
    
    private void reset(boolean smallHash, int numHashEntries, int numCollisionEntries, int stringDataSize) {
        _smallHash = smallHash;
        
        _hashToOffsets = new Int2IntOpenHashMap(numHashEntries);
        _hashToOffsets.defaultReturnValue(MISSING_HASH_VALUE);
        _collisionMap = new HashMap<String, String>(numCollisionEntries);
        
        // The key and value strings are stored as null-termianted UTF-8 bytes
        _stringData = new byte[stringDataSize];
        _curStringOffset = 0;
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
        for (; _curStringOffset < stringDataSize; ) {
            int keyLen = calcStringLength(_curStringOffset);
            if (keyLen > 0) {
                // only process strings we haven't deleted
                int hash = hash(_stringData, _curStringOffset, keyLen);
                int oldOffset = _hashToOffsets.put(hash, _curStringOffset);
                if (oldOffset != MISSING_HASH_VALUE) {
                    throw new IOException("Data corruption - hash already exists!");
                }
                
                _curStringOffset += (keyLen + 1);

                // Skip over the value
                int valueLen = calcStringLength(_curStringOffset);
                _curStringOffset += (valueLen + 1);
            } else {
                _curStringOffset += 1;
            }
        }
        
        // Now read in the collision values. For each, make sure we already have a
        // hash entry, otherwise it's an error.
        for (int i = 0; i < numCollisionEntries; i++) {
            String key = in.readUTF();
            String value = in.readUTF();
            
            int hash = hash(key);
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
        out.writeInt(_curStringOffset);
        out.write(_stringData, 0, _curStringOffset);

        // Write out the entries we've saved in the collision set.
        for (Entry<String, String> entry : _collisionMap.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }

    private int calcStringLength(int startingOffset) {
        int curOffset = startingOffset;
        while (_stringData[curOffset] != 0) {
            curOffset += 1;
        }
        
        return curOffset - startingOffset;
    }
    
    private String getValueString(int valueOffset, int valueLen) {
        try {
            return new String(_stringData, valueOffset, valueLen, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Impossible missing charset exception", e);
        }
    }
    
    /**
     * Generate a 32-bit JOAAT hash from the bytes of <phrase>
     * 
     * @param phrase String to hash
     * @return 32-bit hash
     */
    public int hash(String phrase) {
        int result = HashUtils.getIntHash(phrase);
        
        if (_smallHash) {
            // only generate 256 unique hash values, for testing.
            result = result & 0x0FF;
        }
        
        return result;
    }
    
    private int hash(byte[] b, int offset, int length) {
        int result = HashUtils.getIntHash(b, offset, length);
        
        if (_smallHash) {
            // only generate 256 unique hash values, for testing.
            result = result & 0x0FF;
        }
        
        return result;
    }
    
    @Override
    public int size() {
        return _hashToOffsets.size() + _collisionMap.size();
    }

    @Override
    public boolean isEmpty() {
        return _hashToOffsets.isEmpty() && _collisionMap.isEmpty();
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
                int hash = hash((String)key);
                int keyOffset = _hashToOffsets.remove(hash);
                if (keyOffset != MISSING_HASH_VALUE) {
                    // We need to clear out the entry so we don't re-add it as a string
                    // when we de-serialize things.
                    int keyLen = calcStringLength(keyOffset);
                    
                    int valueOffset = keyOffset + keyLen + 1;
                    int valueLen = calcStringLength(valueOffset);
                    String result = getValueString(valueOffset, valueLen);
                    Arrays.fill(_stringData, keyOffset, keyOffset + keyLen + 1 + valueLen + 1, (byte)0);
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
        if (_stringData.length > STRING_DATA_BLOCKSIZE) {
            _stringData = new byte[STRING_DATA_BLOCKSIZE];
        }
        
        _curStringOffset = 0;
    }

    @Override
    public boolean containsKey(Object key) {
        if (key instanceof String) {
            int hash = hash((String)key);
            int keyOffset = _hashToOffsets.get(hash);
            if (keyOffset == MISSING_HASH_VALUE) {
                return false;
            }
            
            // We might have a match...need to see if the actual string matches our stored bytes.
            // If not, then we check the collision set.
            byte[] stringBytes = HashUtils.getUTF8Bytes((String)key);
            boolean matches = true;
            for (int i = 0; (i < stringBytes.length) && matches; i++) {
                if (stringBytes[i] != _stringData[keyOffset + i]) {
                    matches = false;
                }
            }
            
            // If it matched all of the string bytes, make sure we've got our terminating null byte.
            matches = matches && _stringData[keyOffset + stringBytes.length] == 0;
            
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
        int hash = hash((String)key);
        int keyOffset = _hashToOffsets.get(hash);
        if (keyOffset == MISSING_HASH_VALUE) {
            return null;
        }

        byte[] stringBytes = HashUtils.getUTF8Bytes((String)key);
        boolean matches = true;
        for (int i = 0; (i < stringBytes.length) && matches; i++) {
            if (stringBytes[i] != _stringData[keyOffset + i]) {
                matches = false;
            }
        }
        
        // If it matched all of the string bytes, make sure we've got our terminating null byte.a
        matches = matches && _stringData[keyOffset + stringBytes.length] == 0;
        
        if (matches) {
            int keyLen = stringBytes.length;
            int valueOffset = keyOffset + keyLen + 1;
            int valueLen = calcStringLength(valueOffset);
            return getValueString(valueOffset, valueLen);
        } else {
            return _collisionMap.get(key);
        }
    }

    private boolean keyInHash(String key) {
        int hash = hash(key);
        int keyOffset = _hashToOffsets.get(hash);
        if (keyOffset == MISSING_HASH_VALUE) {
            return false;
        }

        byte[] stringBytes = HashUtils.getUTF8Bytes(key);
        boolean matches = true;
        for (int i = 0; (i < stringBytes.length) && matches; i++) {
            if (stringBytes[i] != _stringData[keyOffset + i]) {
                matches = false;
            }
        }
        
        // If it matched all of the string bytes, make sure we've got our terminating null byte.a
        matches = matches && _stringData[keyOffset + stringBytes.length] == 0;
        return matches;
    }
    
    @Override
    public String put(String key, String value) {
        int hash = hash(key);
        int keyOffset = _hashToOffsets.get(hash);
        if (keyOffset == MISSING_HASH_VALUE) {
            // We need to add it to the array and the hash set
            byte[] keyBytes = HashUtils.getUTF8Bytes(key);
            byte[] valueBytes = HashUtils.getUTF8Bytes(value);
            
            // Make sure we have enough space in the array.
            int endOffset = _curStringOffset + keyBytes.length + 1 + valueBytes.length + 1;
            if (endOffset > _stringData.length) {
                byte[] newData = new byte[endOffset + STRING_DATA_BLOCKSIZE];
                System.arraycopy(_stringData, 0, newData, 0, _curStringOffset);
                _stringData = newData;
            }
            
            _hashToOffsets.put(hash, _curStringOffset);

            System.arraycopy(keyBytes, 0, _stringData, _curStringOffset, keyBytes.length);
            _curStringOffset += keyBytes.length;
            _stringData[_curStringOffset++] = 0;
            
            System.arraycopy(valueBytes, 0, _stringData, _curStringOffset, valueBytes.length);
            _curStringOffset += valueBytes.length;
            _stringData[_curStringOffset++] = 0;

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
