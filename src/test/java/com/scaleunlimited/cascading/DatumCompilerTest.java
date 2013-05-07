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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.Date;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.JavaSourceClassLoader;
import org.junit.Test;

import com.scaleunlimited.cascading.DatumCompiler.CompiledDatum;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


public class DatumCompilerTest extends Assert {

        
        // TODO add test for this forcing the creation of a transient datum,
        // and having abstract get/set methods.
        // private transient List<String> _expandedAliases;
    
    @Test
    public void testSimpleSchema() throws Exception {
        CompiledDatum result = DatumCompiler.generate(MyDatumTemplate.class);
        
        File baseDir = FileUtils.getTempDirectory();
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

        Constructor c = clazz.getConstructor(String.class, int.class, Date.class, Tuple.class);
        Object datum = c.newInstance("robert", 25, new Date(), new Tuple("bob", "rob"));
    }
}
