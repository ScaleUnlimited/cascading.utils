package com.scaleunlimited.cascading;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.tools.ant.AntClassLoader;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;

import com.scaleunlimited.cascading.DatumCompiler.CompiledDatum;

public class DatumCompilerAntTask extends Task {

    private String _classname;
    private String _srcDir;
    
    public void execute() throws BuildException {
        
        File outputFile;
        CompiledDatum compiledDatum;
        
        AntClassLoader cl = new AntClassLoader(getClass().getClassLoader(), true);

        try {
            // Class clazz = loader.loadClass(_classname);

            Class clazz = cl.loadClass(_classname);
            compiledDatum = DatumCompiler.generate(clazz);
            
            File baseDir = new File(_srcDir);
            if (!baseDir.exists()) {
                throw new BuildException("Destination source dir doesn't exist: " + baseDir.getAbsolutePath());
            }
            
            if (!baseDir.isDirectory()) {
                throw new BuildException("Destination source dir isn't a directory: " + baseDir.getAbsolutePath());
            }
            
            File outputDir = new File(baseDir, compiledDatum.getPackageName().replaceAll("\\.", "/"));
            if (!outputDir.mkdirs()) {
                throw new BuildException("Can't create output directory for class: " + outputDir.getAbsolutePath());
            }
            
            outputFile = new File(outputDir, compiledDatum.getClassName() + ".java");
        } catch (ClassNotFoundException e) {
            throw new BuildException("Can't find datum reference class: " + _classname, e);
        }
        
        OutputStream os = null;
        
        try {
            os = new FileOutputStream(outputFile);
            IOUtils.write(compiledDatum.getClassCode(), os, "UTF-8");
        } catch (FileNotFoundException e) {
            throw new BuildException("Unable to create output file: " + outputFile, e);
        } catch (IOException e) {
            throw new BuildException("Unable to write to output file: " + outputFile, e);
        } finally {
            IOUtils.closeQuietly(os);
        }
    }

    public void setClassname(String classname) {
        _classname = classname;
    }
    
    public void setSrcDir(String srcDir) {
        _srcDir = srcDir;
    }

}
