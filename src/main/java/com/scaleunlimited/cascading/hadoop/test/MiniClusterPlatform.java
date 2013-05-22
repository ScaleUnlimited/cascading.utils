package com.scaleunlimited.cascading.hadoop.test;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
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
        File logDir = new File(logDirName);
        logDir.mkdirs();
        FileUtils.deleteDirectory(logDir);

        if (tempDirName == null) {
            File tempDir = new File(sysTmpDir, DEFAULT_TEMPDIR_NAME);
            tempDirName = tempDir.getAbsolutePath(); 
        }
        
        // Always set the temp dir location, otherwise it winds up being /tmp/hadoop-${user.name}
        System.setProperty("hadoop.tmp.dir", tempDirName);
        File tempDir = new File(tempDirName);
        tempDir.mkdirs();
        FileUtils.deleteDirectory(tempDir);
        
        // MiniMR cluster always uses (CWD-relative) "build/test/mapred/" for its data. Nice.
        // MinDFS cluster uses "build/test/data/", but you can override it via the test.build.data
        // system property, but that doesn't help us much here.
        File dfsDir = new File("build/test/data");
        FileUtils.deleteDirectory(dfsDir);
        File mrDir = new File("build/test/mapred");
        FileUtils.deleteDirectory(mrDir);

        // Get rid of warnings that we get from bogus security settings.
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

        JobConf conf = new JobConf();
        conf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
        conf.set("hadoop.tmp.dir", tempDirName);
        conf.set("hadoop.log.dir", logDirName);
        
        int totalSlots = numMapSlots + numReduceSlots;
        _dfs = new MiniDFSCluster(conf, numMapSlots, true, null);
        _dfs.waitClusterUp();
        
        FileSystem fileSys = _dfs.getFileSystem();
        _mr = new MiniMRCluster(totalSlots, fileSys.getUri().toString(), 1, null, null, conf);
        _mr.setInlineCleanupThreads();
        
        JobConf jobConf = _mr.createJobConf();

        jobConf.set("mapred.child.java.opts", "-Xmx128m");
        jobConf.setInt("mapred.job.reuse.jvm.num.tasks", -1);
        jobConf.setInt("jobclient.completion.poll.interval", 50);
        jobConf.setInt("jobclient.progress.monitor.poll.interval", 50);
        jobConf.setMapSpeculativeExecution(false);
        jobConf.setReduceSpeculativeExecution(false);

        jobConf.setNumMapTasks(numMapSlots);
        jobConf.setNumReduceTasks(numReduceSlots);

        // Update _conf to match what we get back from the minicluster
        _conf = new JobConf(jobConf);
    }

    public void shutdown() throws InterruptedException {
        if (_mr != null) {
            _mr.shutdown();
        }

        if (_dfs != null) {
            _dfs.shutdown();
            while (_dfs.isClusterUp()) {
                Thread.sleep(100L);
            }
        }
    }

}
