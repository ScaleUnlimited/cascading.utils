/**
 * Copyright 2010-2013 TransPac Software, Inc.
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.Date;
import java.util.UUID;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.codehaus.janino.JavaSourceClassLoader;
import org.junit.Test;

import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.DatumCompiler.CompiledDatum;
import com.scaleunlimited.cascading.hadoop.HadoopPlatform;


public class DatumCompilerTest extends Assert {

        
        // TODO add test for this forcing the creation of a transient datum,
        // and having abstract get/set methods.
        // private transient List<String> _expandedAliases;
    
    @Test
    public void testSimpleSchema() throws Exception {
        CompiledDatum result = DatumCompiler.generate(MyDatumTemplate.class);
        
        File baseDir = new File("build/test/DatumCompilerTest/testSimpleSchema/");
        FileUtils.deleteDirectory(baseDir);
        File srcDir = new File(baseDir, result.getPackageName().replaceAll("\\.", "/"));
        assertTrue(srcDir.mkdirs());
        
        File codeFile = new File(srcDir, result.getClassName() + ".java");
        OutputStream os = new FileOutputStream(codeFile);
        IOUtils.write(result.getClassCode(), os, "UTF-8");
        os.close();
        
        // Compile with Janino, give it a try. We have Janino since
        // it's a cascading dependency, but probably want to add a test
        // dependency on it.
        
        ClassLoader cl = new JavaSourceClassLoader(
                        this.getClass().getClassLoader(),  // parentClassLoader
                        new File[] { baseDir }, // optionalSourcePath
                        (String) null                     // optionalCharacterEncoding
                    );
        
        
        // WARNING - we have to use xxxDatumTemplate as the base name, so that the code returned
        // by the compiler is for type xxxDatum. Otherwise when we try to load the class here,
        // we'll likely get the base (template) class, which will mask our generated class.
        Class clazz = cl.loadClass(result.getPackageName() + "." + result.getClassName());
        assertEquals("MyDatum", clazz.getSimpleName());
        
        // Verify that we have a constructor which takes all of the fields.
//        private String _name;
//        private int ageAndRisk;
//        private Date _date;
//        private Tuple _aliases;
//        private String[] _phoneNumbers
        // private MyDatumEnum _enum
        
        Constructor c = clazz.getConstructor(String.class, int.class, Date.class, Tuple.class, String[].class, MyDatumEnum.class);
        BaseDatum datum = (BaseDatum)c.newInstance("robert", 25, new Date(), new Tuple("bob", "rob"), new String[]{"555-1212", "555-4848"},
                        MyDatumEnum.DATUM_COMPILER_ENUM_2);
        
        // Verify that it can be serialized with Hadoop.
        // TODO figure out why Hadoop serializations aren't available???
        /*
        BasePlatform testPlatform = new HadoopPlatform(DatumCompilerTest.class);
        Tap tap = testPlatform.makeTap( testPlatform.makeBinaryScheme(datum.getFields()), 
                        testPlatform.makePath("build/test/DatumCompilerTest/testSimpleSchema/"));
        TupleEntryCollector writer = tap.openForWrite(testPlatform.makeFlowProcess());
        writer.add(datum.getTuple());
        writer.close();

        TupleEntryIterator iter = tap.openForRead(testPlatform.makeFlowProcess());
        TupleEntry te = iter.next();
        
        // TODO how to test round-trip?
         */
    }
    
    @Test
    public void testUUID() throws Exception {
        CompiledDatum result = DatumCompiler.generate(MyUUIDDatumTemplate.class);
        
        File baseDir = new File("build/test/DatumCompilerTest/testUUID/");
        FileUtils.deleteDirectory(baseDir);
        File srcDir = new File(baseDir, result.getPackageName().replaceAll("\\.", "/"));
        assertTrue(srcDir.mkdirs());
        
        File codeFile = new File(srcDir, result.getClassName() + ".java");
        OutputStream os = new FileOutputStream(codeFile);
        IOUtils.write(result.getClassCode(), os, "UTF-8");
        os.close();
        
        // Compile with Janino, give it a try. We have Janino since
        // it's a cascading dependency, but probably want to add a test
        // dependency on it.
        
        ClassLoader cl = new JavaSourceClassLoader(
                        this.getClass().getClassLoader(),  // parentClassLoader
                        new File[] { baseDir }, // optionalSourcePath
                        (String) null                     // optionalCharacterEncoding
                    );
        
        
        // WARNING - we have to use xxxDatumTemplate as the base name, so that the code returned
        // by the compiler is for type xxxDatum. Otherwise when we try to load the class here,
        // we'll likely get the base (template) class, which will mask our generated class.
        Class clazz = cl.loadClass(result.getPackageName() + "." + result.getClassName());
        assertEquals("MyUUIDDatum", clazz.getSimpleName());
        
        Constructor c = clazz.getConstructor(UUID.class);
        BaseDatum datum = (BaseDatum)c.newInstance(UUID.randomUUID());
        
        // Verify that it can be serialized with Hadoop.
        // TODO figure out why Hadoop serializations aren't available???
        /*
        BasePlatform testPlatform = new HadoopPlatform(DatumCompilerTest.class);
        Tap tap = testPlatform.makeTap( testPlatform.makeBinaryScheme(datum.getFields()), 
                        testPlatform.makePath("build/test/DatumCompilerTest/testSimpleSchema/"));
        TupleEntryCollector writer = tap.openForWrite(testPlatform.makeFlowProcess());
        writer.add(datum.getTuple());
        writer.close();

        TupleEntryIterator iter = tap.openForRead(testPlatform.makeFlowProcess());
        TupleEntry te = iter.next();
        
        // TODO how to test round-trip?
         */
    }

}
