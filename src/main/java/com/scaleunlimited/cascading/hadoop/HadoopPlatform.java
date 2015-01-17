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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
import cascading.tap.hadoop.PartitionTap;
import cascading.tap.hadoop.TemplateTap;
import cascading.tap.partition.Partition;
import cascading.tuple.Fields;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.Level;

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
    public File getLogDir() {
        return super.getLogDirHelper();
    }

    @Override
    public Tap makeTap(Scheme scheme, BasePath path) throws Exception {
        return makeTap(scheme, path, SinkMode.KEEP);
    }

    @Override
    public void setJobPollingInterval(long interval) {
        super.setJobPollingIntervalHelper(interval);
    }

    @Override
    public void setLogDir(File logDir) {
        super.setLogDirHealer(logDir);
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
    public String getProperty(String name) {
        String result = super.getPropertyHelper(name);
        if (result == null) {
            result = _conf.get(name);
        }
        
        return result;
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
    public Tap makePartitionTap(Tap parentTap, Partition partition) throws Exception {
        return makePartitionTap(parentTap, partition, SinkMode.KEEP);
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
    public void setLogLevel(Level level, String... packageNames) {
        for (String packageName : packageNames) {
            if (packageName.isEmpty()) {
                // TODO set the logging level for the current (main) code that's calling
                // us to - but we can't do that (???) using slf4j, so this would only work
                // if we assume log4j is being used.
                
                // Set the logging level for map & reduce jobs, using both old and new conf names.
                _conf.set("mapred.map.child.log.level", level.toString());
                _conf.set("mapreduce.map.log.level", level.toString());
                _conf.set("mapred.reduce.child.log.level", level.toString());
                _conf.set("mapreduce.reduce.log.level", level.toString());
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
    public FlowConnector makeFlowConnector() {
        // Combine _props with JobConf.
        Map<Object, Object> hadoopProps = HadoopUtil.createProperties(_conf);
        Map<Object, Object> mergedProps = new HashMap<Object, Object>(_props);
        
        for (Map.Entry<Object, Object> hadoopEntry : hadoopProps.entrySet()) {
            Object key = hadoopEntry.getKey();
            Object hadoopValue = hadoopEntry.getValue();
            Object explicitValue = mergedProps.get(key);
            
            // If we have a Hadoop property value, and there isn't something already
            // set for that explicitly in our properties, then use it.
            if ((hadoopValue != null) && (explicitValue == null)) {
                mergedProps.put(key, hadoopValue);
            }
        }
        
        return new HadoopFlowConnector(mergedProps);
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
    public Tap makePartitionTap(Tap parentTap, Partition partition, SinkMode mode) throws Exception {
        if (parentTap instanceof Hfs) {
            Hfs tap = (Hfs) parentTap;
            return new PartitionTap(tap, partition, mode);
        }
        throw new RuntimeException("parentTap needs to an instance of Hfs - instead got: " + parentTap.getClass().getName());
    }
    
    @Override
    public Scheme makeBinaryScheme(Fields fields) {
        return new SequenceFile(fields);
    }

    @Override
    public Scheme makeTextScheme(boolean enableCompression) {
        if (enableCompression) {
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
        DataOutputStream writeableOut = new DataOutputStream(byteStream);
        _conf.write(writeableOut);
        writeableOut.close();
        
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

        _conf = new JobConf(false);
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
            // Make sure every value we've got exists and is equal to the other
            // value. We can't do a two-way comparison, because when JobConf
            // deserializes, it adds additional properties (aliases) for what we
            // set.
            Iterator<Entry<String, String>> iter = other._conf.iterator();
            Map<String, String> otherValues = new HashMap<String, String>();
            
            while (iter.hasNext()) {
                Entry<String, String> entry = iter.next();
                otherValues.put(entry.getKey(), entry.getValue());
            }
            
            
            iter = _conf.iterator();
            while (iter.hasNext()) {
                Entry<String, String> entry = iter.next();
                if (!otherValues.containsKey(entry.getKey())) {
                    return false;
                } else if (!entry.getValue().equals(otherValues.get(entry.getKey()))) {
                    return false;
                }
            }
        }
        
        return true;
    }

    
}
