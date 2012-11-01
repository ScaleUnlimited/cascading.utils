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

import junit.framework.Assert;

import org.junit.Test;

import com.scaleunlimited.cascading.BaseDatum;

import cascading.tuple.Fields;


public class DatumTest {

    @SuppressWarnings("serial")
    private static class MyDatum extends BaseDatum {

        public static final Fields FIELDS = new Fields("a", "b");
        
        public MyDatum() {
            super(FIELDS);
        }
        
        public MyDatum(Fields fields) {
            super(fields);
            validateFields(fields, FIELDS);
        }
    }
    
    @Test
    public void testSubclassing() throws Exception {
        MyDatum md = new MyDatum();
        Assert.assertEquals(MyDatum.FIELDS, md.getFields());
    }
    
    @Test
    public void testValidation() throws Exception {
        try {
            new MyDatum(MyDatum.FIELDS);
        } catch (Exception e) {
            Assert.fail("Should not have thrown exception");
        }
        
        try {
            new MyDatum(new Fields("c"));
            Assert.fail("Should have thrown exception");
        } catch (Exception e) {
        }
        
        
    }
}
