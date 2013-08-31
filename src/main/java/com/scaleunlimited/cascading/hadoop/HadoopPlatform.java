package com.scaleunlimited.cascading.hadoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import cascading.tap.hadoop.TemplateTap;
import cascading.tuple.Fields;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;

@SuppressWarnings({ "unchecked", "rawtypes", "serial" })
public class HadoopPlatform extends BasePlatform {
    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopPlatform.class);

    public static final String PLATFORM_TYPE = "hadoop";

    protected JobConf _conf;
    
    public HadoopPlatform(Class applicationJarClass) {
        this(applicationJarClass, new JobConf());
    }
    
    @Override
    public String getPlatformType() {
        return PLATFORM_TYPE;
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
        if (isLocal()) {
            return new HadoopPath("file://" + FileUtils.getTempDirectoryPath());
        } else {
            return new HadoopPath(Hfs.getTempPath(getJobConf()).getName(), _conf);
        }
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
    public int getNumReduceTasks() throws Exception {
        return HadoopUtils.getNumReducers(getJobConf());
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
    public Tap makeTemplateTap(Tap tap, String pattern, Fields fields) throws Exception {
        return new TemplateTap((Hfs) tap, pattern, fields);
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
            return new TextLine(Compress.DISABLE);
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
    
    @Override
    public String shareLocalDir(String localDirName) {

        String sharedDirName = null;

        try {
            Path localPath = new Path(localDirName);
            JobConf conf = (JobConf) (makeFlowProcess().getConfigCopy());
            String hadoopTmpDirName = conf.getJobLocalDir();
            if (hadoopTmpDirName == null) {
                hadoopTmpDirName = conf.get("hadoop.tmp.dir");
            }
            if (hadoopTmpDirName == null) {
                hadoopTmpDirName = conf.getWorkingDirectory().toString();
            }
            if (hadoopTmpDirName == null) {
                throw new IOException("Can't get Hadoop temporary directory");
            }
            HadoopPath hadoopHdfsTmpPath = (HadoopPath) (makePath(hadoopTmpDirName));
            Path hdfsTmpPath = hadoopHdfsTmpPath.getHadoopPath();
            String uniqueFolderName = String.format("%s-%s", localPath.getName(), UUID.randomUUID());
            Path sharedPath = new Path(hdfsTmpPath, uniqueFolderName);
            sharedDirName = sharedPath.toString();
            FileSystem targetFs = sharedPath.getFileSystem(conf);

            // Copy directory to (shared) HDFS location.
            targetFs.copyFromLocalFile(localPath, sharedPath);
            String message = String.format("Successfully copied shared directory from %s to %s", localDirName, sharedDirName);
            LOGGER.info(message);
        } catch (Exception e) {
            String message = String.format("Exception sharing directory from %s to %s: %s", localDirName, sharedDirName, e);
            LOGGER.error(message, e);
            throw new RuntimeException(message);
        }

        return sharedDirName;
    }
    
    @Override
    public String copySharedDirToLocal(FlowProcess flowProcess, String sharedDirName) {
        String localDirName = null;

        try {
            // Main program on Hadoop master has written the directory to HDFS,
            // so copy it to the slave's local hard drive.
            Path sourcePath = new Path(sharedDirName);
            JobConf conf = (JobConf) (flowProcess.getConfigCopy());
            FileSystem sourceFs = sourcePath.getFileSystem(conf);
            String tmpDirName = System.getProperty("java.io.tmpdir");
            String uniqueFolderName = String.format("%s-%s", sourcePath.getName(), UUID.randomUUID());
            File localDir = new File(tmpDirName, uniqueFolderName);
            localDirName = localDir.getAbsolutePath();
            Path localPath = new Path(localDirName);

            // Copy directory from (shared) HDFS location.
            sourceFs.copyToLocalFile(sourcePath, localPath);
            String message = String.format("Successfully copied shared directory from %s to %s", sharedDirName, localDirName);
            LOGGER.info(message);

        } catch (IOException e) {
            String message = String.format("Exception copying shared directory from %s to %s: %s", sharedDirName, localDirName, e);
            LOGGER.error(message, e);
            throw new RuntimeException(message);
        }
        return localDirName;
    }

    /**
     * JobConf isn't serializable, so we handle that ourselves.
     * 
     * @param out
     * @throws IOException
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutput writeableOut = new DataOutputStream(byteStream);
        _conf.write(writeableOut);
        
        // Now write out the byte array
        byte[] confBytes = byteStream.toByteArray();
        out.writeInt(confBytes.length);
        out.write(confBytes, 0, confBytes.length);
    }
    
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        int numBytes = in.readInt();
        byte[] confBytes = new byte[numBytes];
        in.readFully(confBytes);
        DataInput writeableIn = new DataInputStream(new ByteArrayInputStream(confBytes));

        _conf = new JobConf();
        _conf.readFields(writeableIn);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        
        HadoopPlatform other = (HadoopPlatform) obj;
        if (_conf == null) {
            if (other._conf != null)
                return false;
        } else {
            // TODO - implement real equals for JobConf
            return true;
        }
        
        return true;
    }

    
}
