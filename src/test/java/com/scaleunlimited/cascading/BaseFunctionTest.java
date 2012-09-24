package com.scaleunlimited.cascading;

import static org.junit.Assert.*;

import org.junit.Test;

import com.scaleunlimited.cascading.BaseDatum;
import com.scaleunlimited.cascading.BaseFunction;

import cascading.tuple.Fields;

public class BaseFunctionTest {

    private static class MyDatum extends BaseDatum {
        
        public MyDatum() {
            super(new Fields("field"));
        }
    }
    
    private static class MyFunction extends BaseFunction<MyDatum, MyDatum> {

        public MyFunction(Class<MyDatum> inClass, Class<MyDatum> outClass) throws Exception {
            super(inClass, outClass);
            // TODO Auto-generated constructor stub
        }

        @Override
        void process(MyDatum in) throws Exception {
            // TODO Auto-generated method stub
            
        }
        
    }
    
    @Test
    public void test() throws Exception {
        MyFunction f = new MyFunction(MyDatum.class, MyDatum.class);

    }

}
