package com.scaleunlimited.cascading.hadoop.test;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;

import cascading.util.Util;

import com.scaleunlimited.cascading.hadoop.HadoopPlatform;

@SuppressWarnings("rawtypes")
public class MiniClusterPlatform extends HadoopPlatform {

    private static final int DEFAULT_NUM_MAP_SLOTS = 2;
    private static final int DEFAULT_NUM_REDUCE_SLOTS = 2;
    private static final String DEFAULT_LOGDIR_NAME = "minicluster-logs";
    private static final String DEFAULT_TEMPDIR_NAME = "minicluster-tmp";
    
    private MiniMRCluster _mr = null;
    MiniDFSCluster _dfs = null;
    
    public MiniClusterPlatform(Class applicationJarClass) throws IOException {
        this(applicationJarClass, DEFAULT_NUM_MAP_SLOTS, DEFAULT_NUM_REDUCE_SLOTS, null);
    }

    public MiniClusterPlatform(Class applicationJarClass, int numMapSlots, int numReduceSlots, String logDirName) throws IOException {
        this(applicationJarClass, numMapSlots, numReduceSlots, logDirName, null);
    }

    public MiniClusterPlatform(Class applicationJarClass, int numMapSlots, int numReduceSlots, String logDirName, String tempDirName) throws IOException {
        super(applicationJarClass);
        setupMiniClusterPlatform(numMapSlots, numReduceSlots, logDirName, tempDirName);
    }

    
    private void setupMiniClusterPlatform(int numMapSlots, int numReduceSlots, String logDirName, String tempDirName) throws IOException {
        
        String sysTmpDir = System.getProperty("java.io.tmpdir");

        if (logDirName == null) {
            File logDir = new File(sysTmpDir, DEFAULT_LOGDIR_NAME);
            logDirName = logDir.getAbsolutePath();
        }
        
        System.setProperty("hadoop.log.dir", logDirName);
        new File( System.getProperty("hadoop.log.dir")).mkdirs();

        if (tempDirName == null) {
            File tempDir = new File(sysTmpDir, DEFAULT_TEMPDIR_NAME);
            tempDirName = tempDir.getAbsolutePath(); 
        }
        
        if (Util.isEmpty(System.getProperty("hadoop.tmp.dir"))) {
            System.setProperty("hadoop.tmp.dir", tempDirName);
            new File( System.getProperty("hadoop.tmp.dir")).mkdirs();
        }
        
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");


        JobConf conf = new JobConf();

        conf.setInt("mapred.job.reuse.jvm.num.tasks", -1 );

        int totalSlots = numMapSlots + numReduceSlots;
        _dfs = new MiniDFSCluster(conf, numMapSlots, true, null);
        FileSystem fileSys = _dfs.getFileSystem();
        _mr = new MiniMRCluster(totalSlots, fileSys.getUri().toString(), 1, null, null, conf);

        JobConf jobConf = _mr.createJobConf();

        jobConf.set("mapred.child.java.opts", "-Xmx128m");
        jobConf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
        jobConf.setInt("jobclient.completion.poll.interval", 50);
        jobConf.setInt("jobclient.progress.monitor.poll.interval", 50);
        jobConf.setMapSpeculativeExecution( false );
        jobConf.setReduceSpeculativeExecution( false );

        jobConf.setNumMapTasks(numMapSlots);
        jobConf.setNumReduceTasks(numReduceSlots);

        // Clear out the existing job conf properties in HadoopPlatform
        _conf = new JobConf(jobConf);
    }

    public void shutdown() {
        if (_dfs != null) {
            _dfs.shutdown();
        }
        if (_mr != null) {
            _mr.shutdown();
        }
    }

}
