/**
 * Copyright 2010 TransPac Software, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.scaleunlimited.cascading;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


/**
 * Subclasses can hide fields inside the payload field tuple,
 * allowing this data to be payloaded through the workflow without
 * it having knowledge of the payload field details (though it may
 * still have to copy the payload field tuple from an instance of
 * one subclass to an instance of another).
 */
@SuppressWarnings("serial")
public class PayloadDatum extends BaseDatum {

    public static final String PAYLOAD_FN = fieldName(PayloadDatum.class, "payload");
    public static final Fields FIELDS = new Fields(PAYLOAD_FN).append(BaseDatum.getSuperFields(PayloadDatum.class));

    private transient Payload _payload;
    private transient boolean _updated = false;
    
    public PayloadDatum() {
        super(FIELDS);
    }
    
    public PayloadDatum(Fields fields) {
        super(fields);
        validateFields(fields, FIELDS);
    }

    public PayloadDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
        validateFields(fields, FIELDS);
    }

    public PayloadDatum(TupleEntry tupleEntry) {
        super(tupleEntry);
        validateFields(tupleEntry.getFields(), FIELDS);
    }
    
    @Override
    public void commit() {
        super.commit();

        if (_updated) {
            _tupleEntry.set(PAYLOAD_FN, _payload.toTuple());
            _updated = false;
        }
    }
    
    @Override
    public void reset() {
        setPayload((Payload)null);
    }
    
    public Payload getPayload() {
        if (_payload == null) {
            _payload = new Payload((Tuple)_tupleEntry.get(PAYLOAD_FN));
            _updated = false;
        }
        
        return _payload;
    }

    public Object getPayloadValue(String key) {
        return getPayload().get(key);
    }
    
    /**
     * Set the payload to be the passed information.
     * 
     * @param payload new payload (which can be null)
     */
    public void setPayload(Payload payload) {
        _payload = payload;
        _updated = payload != null;
    }
    
    public void setPayload(PayloadDatum datum) {
        setPayload(new Payload(datum.getPayload()));
    }
    
    public void setPayloadValue(String key, Object value) {
        getPayload().put(key, value);
        _updated = true;
    }
}
