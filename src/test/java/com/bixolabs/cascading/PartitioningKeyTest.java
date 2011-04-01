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

package com.bixolabs.cascading;

import junit.framework.Assert;

import org.junit.Test;

public class PartitioningKeyTest {

    @Test
    public void testValues() throws Exception {
        final int numReducers = 1;
        PartitioningKey key = new PartitioningKey("test", numReducers);
        
        for (int i = 0; i < 100; i++) {
            PartitioningKey otherKey = new PartitioningKey("test-" + i, numReducers);
            Assert.assertEquals(key.getValue(), otherKey.getValue());
        }
    }
}
