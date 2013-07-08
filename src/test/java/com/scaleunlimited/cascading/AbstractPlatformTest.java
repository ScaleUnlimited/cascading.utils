package com.scaleunlimited.cascading;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import junit.framework.Assert;

public abstract class AbstractPlatformTest extends Assert {

    public void testSerialization(BasePlatform platform) throws Exception {
        platform.setJobPollingInterval(666);
        platform.setNumReduceTasks(23);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(baos);
        out.writeObject(platform);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream in = new ObjectInputStream(bais);
        BasePlatform newPlatform = (BasePlatform) in.readObject();
        
        assertEquals(platform, newPlatform);
        
        assertEquals(platform.getDefaultLogDir(), newPlatform.getDefaultLogDir());
    }
}
