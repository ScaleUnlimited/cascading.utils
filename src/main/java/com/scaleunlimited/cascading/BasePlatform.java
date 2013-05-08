package com.scaleunlimited.cascading;

import java.io.File;
import java.security.InvalidParameterException;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Level;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.FlowProps;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

@SuppressWarnings("rawtypes")
public abstract class BasePlatform {

    public static final int CLUSTER_REDUCER_COUNT = -1;
    
    public enum FlowPriority {
        LOW, MEDIUM, HIGH
    }

    // Cascading properties, used when constructing the FlowConnector
    protected Map<Object, Object> _props;
    
    protected File _defaultLogDir;
    
    public BasePlatform(Class applicationJarClass) {
        _props = new Properties();
        
        // We assume that this is getting called from some main() method, so Cascading
        // can find the application main class.
        AppProps.setApplicationJarClass(_props, applicationJarClass);
    }
    
    public void setProperty(String name, String value) {
        _props.put(name, value);
    }
    
    public void setProperty(String name, int value) {
        _props.put(name, value);
    }
    
    public String getProperty(String name) {
        return (String)_props.get(name);
    }
    
    public int getIntProperty(String name) {
        return (Integer)_props.get(name);
    }
    
    public void setJobPollingInterval(long interval) {
        FlowProps.setJobPollingInterval(_props, interval);
    }
    
    public void setLogLevel(Level level, String... packageNames) {
        // TODO check for existing level set for package name, and just update
        String curLogSettings = (String)_props.get("log4j.logger");
        if  ((curLogSettings == null) || (curLogSettings.trim().isEmpty())) {
            curLogSettings = "";
        }
        
        StringBuilder newLogSettings = new StringBuilder(curLogSettings);
        
        for (String packageName : packageNames) {
            if (newLogSettings.length() > 0) {
                newLogSettings.append(',');
            }
            
            newLogSettings.append(packageName);
            newLogSettings.append('=');
            newLogSettings.append(level.toString());
        }
        
        _props.put("log4j.logger", newLogSettings.toString());
    }
    
    public void assertPathExists(BasePath path, String description) {
        if (!path.exists()) {
            throw new InvalidParameterException(String.format("%s doesn't exist: %s", description, path));
        }
    }
    
    public Tap makeTap(Scheme scheme, BasePath path) throws Exception {
        return makeTap(scheme, path, SinkMode.KEEP);
    }

    public abstract boolean isLocal();
    
    // This is protected so that concrete implementations have to implement their
    // version, but they can call the base to see if it's been set to an explicit value.
    protected File getDefaultLogDir() {
        return _defaultLogDir;
    }
    
    public void setDefaultLogDir(File defaultLogDir) {
        _defaultLogDir = defaultLogDir;
    }
    
    public abstract boolean isTextSchemeCompressable();

    public abstract void setNumReduceTasks(int numReduceTasks) throws Exception;

    public abstract void setFlowPriority(FlowPriority priority) throws Exception;

    public abstract FlowConnector makeFlowConnector() throws Exception;

    public abstract FlowProcess makeFlowProcess() throws Exception;
    
    public abstract BasePath makePath(String path) throws Exception;

    public abstract BasePath makePath(BasePath parent, String subdir) throws Exception;

    public abstract Tap makeTap(Scheme scheme, BasePath path, SinkMode mode) throws Exception;

    public abstract Scheme makeBinaryScheme(Fields fields) throws Exception;

    public abstract Scheme makeTextScheme(boolean isEnableCompression) throws Exception;
    
    public abstract Scheme makeTextScheme() throws Exception;
}
