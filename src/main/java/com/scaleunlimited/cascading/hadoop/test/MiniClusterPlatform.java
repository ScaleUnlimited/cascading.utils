package com.scaleunlimited.cascading.hadoop.test;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;

import com.scaleunlimited.cascading.hadoop.HadoopPlatform;

@SuppressWarnings({ "rawtypes", "serial" })
public class MiniClusterPlatform extends HadoopPlatform {

    private static final String DEFAULT_LOGDIR_NAME = "minicluster-logs";
//    private static final String DEFAULT_TEMPDIR_NAME = "minicluster-tmp";
    
    private MiniMRClientCluster _mr2 = null;
    MiniDFSCluster _dfs = null;
    
    public MiniClusterPlatform(Class applicationJarClass) throws IOException {
        this(applicationJarClass, 1);
    }

    public MiniClusterPlatform(Class applicationJarClass, int numContainers) throws IOException {
        this(applicationJarClass, numContainers, null);
    }

    public MiniClusterPlatform(Class applicationJarClass, int numContainers, String logDirName) throws IOException {
        super(applicationJarClass);
        setupMiniClusterPlatform(numContainers, logDirName);
    }

    
    private void setupMiniClusterPlatform(int numContainers, String logDirName) throws IOException {
        
        String sysTmpDir = System.getProperty("java.io.tmpdir");

        if (logDirName == null) {
            File logDir = new File(sysTmpDir, DEFAULT_LOGDIR_NAME);
            logDirName = logDir.getAbsolutePath();
        }
        
        
//        // TODO figure out if we need to set System properties.
//        System.setProperty("hadoop.log.dir", logDirName);
//        System.setProperty("yarn.nodemanager.log-dirs", logDirName);
        File logDir = new File(logDirName);
        logDir.mkdirs();
        FileUtils.deleteDirectory(logDir);
        setLogDir(logDir);
        
//
//        if (tempDirName == null) {
//            File tempDir = new File(sysTmpDir, DEFAULT_TEMPDIR_NAME);
//            tempDirName = tempDir.getAbsolutePath(); 
//        }
//        
//        // Always set the temp dir location, otherwise it winds up being /tmp/hadoop-${user.name}
//        System.setProperty("hadoop.tmp.dir", tempDirName);
//        System.setProperty("yarn.nodemanager.local-dirs", tempDirName);
//        File tempDir = new File(tempDirName);
//        tempDir.mkdirs();
//        FileUtils.deleteDirectory(tempDir);
        
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

        System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
        Configuration conf = new HdfsConfiguration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dfsDir.getAbsolutePath());
//        conf.set("hadoop.tmp.dir", tempDirName);
//        conf.set("yarn.nodemanager.local-dirs", tempDirName);
//        conf.set("hadoop.log.dir", logDirName);
//        conf.set("yarn.nodemanager.log-dirs", logDirName);
        _dfs = new MiniDFSCluster.Builder(conf).build();
        _dfs.waitClusterUp();
        
        _mr2 = MiniMRClientClusterFactory.create(this.getClass(), numContainers, conf);
        _mr2.start();
        
        // Update _conf to match what we get back from the minicluster
        Configuration newConf = _mr2.getConfig();
        _conf = new JobConf(newConf);
    }

    public void shutdown() throws InterruptedException, IOException {
        if (_mr2 != null) {
            _mr2.stop();
        }

        if (_dfs != null) {
            _dfs.shutdown();
            // Note that we don't wait for the cluster to be down, since
            // isClusterUp() always returns true.
        }
    }

}
