package com.scaleunlimited.cascading.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextLine;
import cascading.scheme.hadoop.TextLine.Compress;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class HadoopPlatform extends BasePlatform {

    private JobConf _conf;
    
    public HadoopPlatform(Class applicationJarClass) {
        this(applicationJarClass, new JobConf());
    }
    
    public HadoopPlatform(Class applicationJarClass, JobConf jobConf) {
        super(applicationJarClass);
        
        _conf = jobConf;
    }
    
    public void setMapSpeculativeExecution(boolean speculativeExecution) {
        _conf.setMapSpeculativeExecution(speculativeExecution);
    }
    
    public void setReduceSpeculativeExecution(boolean speculativeExecution) {
        _conf.setReduceSpeculativeExecution(speculativeExecution);
    }
    
    @Override
    public boolean isLocal() {
        return HadoopUtils.isJobLocal(_conf);
    }
    
    @Override
    public boolean isTextSchemeCompressable() {
        // TextLine for Hadoop can read/write compressed files.
        return true;
    }
    
    @Override
    public void setNumReduceTasks(int numReduceTasks) throws Exception {
        if (numReduceTasks == CLUSTER_REDUCER_COUNT) {
            numReduceTasks = HadoopUtils.getNumReducers(_conf);
        }
        
        _conf.setNumReduceTasks(numReduceTasks);
    }

    @Override
    public void setFlowPriority(FlowPriority priority) throws Exception {
        switch (priority) {
            case HIGH:
                _conf.setJobPriority(JobPriority.HIGH);
                break;
                
            case MEDIUM:
                _conf.setJobPriority(JobPriority.NORMAL);
                break;
                
            case LOW:
                _conf.setJobPriority(JobPriority.LOW);
                break;
                
            default:
                throw new RuntimeException("Unknown flow priority: " + priority);
        }
    }

    @Override
    public FlowConnector makeFlowConnector() {
        // Combine _props with JobConf
        Map<Object, Object> hadoopProps = HadoopUtil.createProperties(_conf);
        Map<Object, Object> cascadingProps = new HashMap<Object, Object>(_props);
        
        for (Map.Entry<Object, Object> entry : hadoopProps.entrySet()) {
            cascadingProps.put(entry.getKey(), entry.getValue());
        }
        
        return new HadoopFlowConnector(cascadingProps);
    }

    @Override
    public FlowProcess makeFlowProcess() throws Exception {
        return new HadoopFlowProcess(_conf);
    }
    
    @Override
    public BasePath makePath(String path) throws IOException {
        return new HadoopPath(path, _conf);
    }

    @Override
    public BasePath makePath(BasePath parent, String subdir) throws IOException {
        return new HadoopPath(parent, subdir, _conf);
    }

    @Override
    public Tap makeTap(Scheme scheme, BasePath path, SinkMode mode) {
        return new Hfs(scheme, path.getAbsolutePath(), mode);
    }

    @Override
    public Scheme makeBinaryScheme(Fields fields) {
        return new SequenceFile(fields);
    }

    @Override
    public Scheme makeTextScheme(boolean isEnableCompression) {
        if (isEnableCompression) {
            return new TextLine(Compress.ENABLE);
        } else {
            return makeTextScheme();
        }
    }

    @Override
    public Scheme makeTextScheme() {
        return new TextLine();
    }

}
