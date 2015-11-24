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
import cascading.tap.local.PartitionTap;
import cascading.tap.partition.Partition;
import cascading.tuple.Fields;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.Level;

@SuppressWarnings({ "unchecked", "rawtypes", "serial" })
public class LocalPlatform extends BasePlatform {
    
    public static final String PLATFORM_TYPE = "local";

    public LocalPlatform(Class applicationJarClass) {
        super(applicationJarClass);
    }

    @Override
    public String getPlatformType() {
        return PLATFORM_TYPE;
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
    public String getProperty(String name) {
        return super.getPropertyHelper(name);
    }
    
    @Override
    public boolean getBooleanProperty(String name) {
        return super.getBooleanPropertyHelper(name);
    }
    
    @Override
    public int getIntProperty(String name) {
        return super.getIntPropertyHelper(name);
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
        // We can't control the number of reduce tasks here.
    }

    @Override
    public int getNumReduceTasks() throws Exception {
        return 1;
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
    public void setLogLevel(Level level, String... packageNames) {
        for (String packageName : packageNames) {
            if (packageName.isEmpty()) {
                // TODO set the logging level for the local job...
                // but we can't do that (???) using slf4j, so this would only work
                // if we assume log4j is being used.
            }
        }
        
        super.setLogLevelHelper(level, packageNames);
    }
    
    @Override
    public void setProperty(String name, String value) {
        super.setPropertyHelper(name, value);
    }
    
    @Override
    public void setProperty(String name, int value) {
        super.setPropertyHelper(name, value);
    }
    
    @Override
    public void setProperty(String name, boolean value) {
        super.setPropertyHelper(name, value);
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
    public Tap makePartitionTap(Tap parentTap, Partition partition, SinkMode mode) throws Exception {
        if (parentTap instanceof DirectoryTap) {
            // The local PartitionTap is hard-coded to only work with a FileTap (it expects that the child paths
            // will be paths to files, not directories).
            FileTap tap = new FileTap(parentTap.getScheme(), parentTap.getIdentifier(), parentTap.getSinkMode());
            return new PartitionTap(tap, partition, mode);
        } else if (parentTap instanceof FileTap) {
            FileTap tap = (FileTap)parentTap;
            return new PartitionTap(tap, partition, mode);
        } else {
            throw new RuntimeException("parentTap needs to an instance of FileTap - instead got: " + parentTap.getClass().getName());
        }
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
    public String copySharedDirToLocal( FlowProcess flowProcess, 
                                        String sharedDirName) {
        return sharedDirName;
    }
    
    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public File getLogDir() {
        return super.getLogDirHelper();
    }

    @Override
    public Tap makeTap(Scheme scheme, BasePath path) throws Exception {
        return makeTap(scheme, path, SinkMode.KEEP);
    }

    @Override
    public Tap makePartitionTap(Tap parentTap, Partition partition) throws Exception {
        return makePartitionTap(parentTap, partition, SinkMode.KEEP);
    }

    @Override
    public void setJobPollingInterval(long interval) {
        super.setJobPollingIntervalHelper(interval);
    }

    @Override
    public void setLogDir(File logDir) {
        super.setLogDirHealer(logDir);
    }
}
