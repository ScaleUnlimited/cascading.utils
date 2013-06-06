package com.scaleunlimited.cascading.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import com.scaleunlimited.cascading.local.LocalPath;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class HadoopPlatform extends BasePlatform {

    protected JobConf _conf;
    
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
    
    public JobConf getJobConf() {
        return HadoopUtil.createJobConf(_props, _conf);
    }
    
    @Override
    public boolean isLocal() {
        return HadoopUtils.isJobLocal(getJobConf());
    }
    
    @Override
    public File getDefaultLogDir() {
        String hadoopLogDir = System.getProperty("HADOOP_LOG_DIR");
        if (hadoopLogDir == null) {
            hadoopLogDir = System.getProperty("hadoop.log.dir");
        }

        if (hadoopLogDir == null) {
            String hadoopHomeDir = System.getProperty("HADOOP_HOME");
            if (hadoopHomeDir != null) {
                hadoopLogDir = hadoopHomeDir = "/logs";
            }
        }

        if (hadoopLogDir == null) {
            if (isLocal()) {
                hadoopLogDir = "./";
            } else {
                hadoopLogDir = "/mnt/hadoop/logs/";
            }
        }

        return new File(hadoopLogDir);
    }

    @Override
    public BasePath getTempDir() throws Exception {
        return new HadoopPath(Hfs.getTempPath(getJobConf()).getName(), _conf);
    }

    @Override
    public boolean isTextSchemeCompressable() {
        // TextLine for Hadoop can read/write compressed files.
        return true;
    }
    
    @Override
    public void setNumReduceTasks(int numReduceTasks) throws Exception {
        if (numReduceTasks == CLUSTER_REDUCER_COUNT) {
            numReduceTasks = HadoopUtils.getNumReducers(getJobConf());
        }
        
        _conf.setNumReduceTasks(numReduceTasks);
    }

    @Override
    public void resetNumReduceTasks() throws Exception {
        setNumReduceTasks(CLUSTER_REDUCER_COUNT);
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
        // Combine _props with JobConf. We want the user to call BasePlatform.setProperty to set
        // all Cascading-specific properties, so we shouldn't get any key overlap between the
        // Hadoop JobConf and the Cascading Properties.
        Map<Object, Object> hadoopProps = HadoopUtil.createProperties(_conf);
        Map<Object, Object> cascadingProps = new HashMap<Object, Object>(_props);
        
        for (Map.Entry<Object, Object> entry : hadoopProps.entrySet()) {
            cascadingProps.put(entry.getKey(), entry.getValue());
        }
        
        return new HadoopFlowConnector(cascadingProps);
    }

    @Override
    public FlowProcess makeFlowProcess() throws Exception {
        return new HadoopFlowProcess(getJobConf());
    }
    
    @Override
    public BasePath makePath(String path) throws IOException {
        return new HadoopPath(path, getJobConf());
    }

    @Override
    public BasePath makePath(BasePath parent, String subdir) throws IOException {
        return new HadoopPath(parent, subdir, getJobConf());
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

    @Override
    public boolean rename(BasePath src, BasePath dst) throws Exception {
        Path srcPath = new Path(src.getAbsolutePath());
        Path dstPath = new Path(dst.getAbsolutePath());
        FileSystem fs = srcPath.getFileSystem(getJobConf());

        return fs.rename(srcPath, dstPath);
    }
}
