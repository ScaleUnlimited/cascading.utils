package com.scaleunlimited.cascading.local;

import java.io.Serializable;

import org.objenesis.strategy.StdInstantiatorStrategy;

import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

@SuppressWarnings("serial")
public class KryoContext implements Serializable {

    private Kryo _kryo;
    private Input _input;
    private Output _output;
    private boolean _emptyFile;
    
    private class TupleSerializer extends Serializer<Tuple> {

        @Override
        public Tuple read(Kryo kryo, Input input, Class<Tuple> type) {
            int tupleSize = kryo.readObject(input, Integer.class);
            Tuple result = Tuple.size(tupleSize);
            
            for (int i = 0; i < tupleSize; i++) {
                result.set(i, kryo.readClassAndObject(input));
            }
            
            return result;
        }

        @Override
        public void write(Kryo kryo, Output output, Tuple tuple) {
            kryo.writeObject(output, new Integer(tuple.size()));
            
            // Serialize each tuple element.
            for (int i = 0; i < tuple.size(); i++) {
                kryo.writeClassAndObject(output, tuple.getObject(i));
            }
        }
    }
    
    public KryoContext(Input input, Fields fields) {
        _input = input;
        
        init();

        // We might not have any data, since if somebody calls openForWrite() and then doesn't
        // write anything, you wind up with an empty file.
        _emptyFile = !_input.canReadInt();
        if (!_emptyFile) {
            Fields sinkedFields = _kryo.readObject(_input, Fields.class);
            if (!sinkedFields.contains(fields)) {
                throw new IllegalArgumentException("Source fields not found in sinked data");
            }
        }
    }
    
    public KryoContext(Output output, Fields fields) {
        _output = output;

        init();
        
        // So we can validate on input.
        _kryo.writeObject(_output, fields);
    }
    
    private void init() {
        _kryo = new Kryo();
        
        // Register tuple class so storage is more efficient (no full class names).
        // And set up serializer that knows how to serialize Tuples
        _kryo.register(Tuple.class, new TupleSerializer());
        
        // Support for custom classes w/o empty constructor
        _kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        
        // For some reason Kryo winds up having references that are invalid (id isn't right),
        // so we get null objects.
        // TODO figure out why this is the case.
        _kryo.setReferences(false);
    }
    
    public void serialize(Tuple t) {
        _kryo.writeObjectOrNull(_output, t, Tuple.class);
    }
    
    public Tuple deserialize() {
        if (_emptyFile) {
            return null;
        } else {
            return _kryo.readObjectOrNull(_input, Tuple.class);
        }
    }
    
    public void close() {
        if (_input != null) {
            _input.close();
            _input = null;
        } else if (_output != null) {
            _output.close();
            _output = null;
        }
    }

}
