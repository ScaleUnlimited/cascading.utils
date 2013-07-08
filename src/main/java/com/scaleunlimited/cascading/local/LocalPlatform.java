package com.scaleunlimited.cascading.local;

import java.io.File;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tap.local.TemplateTap;
import cascading.tuple.Fields;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;

@SuppressWarnings({ "unchecked", "rawtypes", "serial" })
public class LocalPlatform extends BasePlatform {

    public LocalPlatform(Class applicationJarClass) {
        super(applicationJarClass);
    }
    
    @Override
    public boolean isLocal() {
        return true;
    }

    @Override
    public File getDefaultLogDir() {
        return new File("./");
    }

    @Override
    public BasePath getTempDir() throws Exception {
        return new LocalPath(FileUtils.getTempDirectoryPath());
    }

    @Override
    public boolean isTextSchemeCompressable() {
        // TextLineScheme can read/write compressed files.
        return true;
    }
    
    @Override
    public void setNumReduceTasks(int numReduceTasks) throws Exception {
        // TODO - bail on setting reduce tasks - control via sinkparts?
        // Nothing to do here, I think...
    }

    @Override
    public void resetNumReduceTasks() throws Exception {
        // As per above, nothing to do here.
    }
    
    @Override
    public void setFlowPriority(FlowPriority priority) throws Exception {
        // Nothing to do here
    }

    @Override
    public FlowConnector makeFlowConnector() throws Exception {
        return new LocalFlowConnector(_props);
    }

    @Override
    public FlowProcess makeFlowProcess() throws Exception {
        // TODO why does this require Properties, but LocalFlowConnector takes map<object, object>?
        Properties props = new Properties();
        for (Entry<Object, Object> entry : _props.entrySet()) {
            if ((entry.getKey() instanceof String) && (entry.getValue() instanceof String)) {
                props.setProperty((String)entry.getKey(), (String)entry.getValue());
            }
        }
        
        return new LocalFlowProcess(props);
    }
    
    @Override
    public BasePath makePath(String path) throws Exception {
        return new LocalPath(path);
    }

    @Override
    public BasePath makePath(BasePath parent, String subdir) throws Exception {
        return new LocalPath(parent, subdir);
    }

    @Override
    public Tap makeTap(Scheme scheme, BasePath path, SinkMode mode) throws Exception {
        return new DirectoryTap(scheme, path.getAbsolutePath(), mode);
    }

    @Override
    public Tap makeTemplateTap(Tap tap, String pattern, Fields fields) throws Exception {
        return new TemplateTap((FileTap) tap, pattern, fields);
    }
    
    @Override
    public Scheme makeBinaryScheme(Fields fields) {
        return new KryoScheme(fields);
    }

    @Override
    public Scheme makeTextScheme(boolean isEnableCompression) {
        if (isEnableCompression) {
            return new TextLineScheme(true);
        } else {
            return makeTextScheme();
        }
    }

    @Override
    public Scheme makeTextScheme() {
        return new TextLineScheme();
    }

    @Override
    public boolean rename(BasePath src, BasePath dst) throws Exception {
        File srcFile = new File(src.getAbsolutePath());
        File dstFile = new File (dst.getAbsolutePath());
        
        return srcFile.renameTo(dstFile);
    }

    @Override
    public String shareLocalDir(String localDirName) {
        
        // Local builds just leave directory in localDirName (which is shared)
        return localDirName;
    }

    @Override
    public String copySharedDirToLocal(  FlowProcess flowProcess, 
                                                String sharedDirName) {
        return sharedDirName;
    }
    
    
    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}
