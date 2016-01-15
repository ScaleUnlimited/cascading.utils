package com.scaleunlimited.cascading.hadoop.test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;

import com.scaleunlimited.cascading.hadoop.HadoopPlatform;

@SuppressWarnings({ "rawtypes", "serial" })
public class MiniClusterPlatform extends HadoopPlatform {

    private static final String MR_IDENTIFIER = "com.scaleunlimited.cascading.MiniClusterPlatform";
    
    private static final String DEFAULT_LOGDIR_NAME = "minicluster-logs";
//    private static final String DEFAULT_TEMPDIR_NAME = "minicluster-tmp";
    
    private MiniYARNCluster _yarn = null;
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
        
        File logDir = new File(logDirName);
        logDir.mkdirs();
        FileUtils.deleteDirectory(logDir);
        setLogDir(logDir);
        
        File dfsDir = new File("build/test/data");
        FileUtils.deleteDirectory(dfsDir);

        // Get rid of warnings that we get from bogus security settings.
        System.setProperty("java.security.krb5.realm", "");
        System.setProperty("java.security.krb5.kdc", "");

        System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
        Configuration conf = new HdfsConfiguration();
        // TODO seems like we'd want to set up other config values, e.g. some candidates are:
        //
        //  yarn.nodemanager.local-dirs - ignored?
        //  yarn.nodemanager.log-dirs - ignored?
        //  dfs.data.dir - ignored, use MiniDFSCluster.HDFS_MINIDFS_BASEDIR
        //  mapreduce.map.java.opts - ???
        //  mapreduce.reduce.java.opts - ???
        //  yarn.scheduler.maximum-allocation-mb - ???
        //  yarn.app.mapreduce.am.resource.mb - ???
        //  yarn.app.mapreduce.am.command-opts - ???
        
        // mapred.child.java.opts: -Xmx200m
        
        //  yarn.scheduler.maximum-allocation-vcores
        // yarn.nodemanager.resource.memory-mb
        
        // We need to set how many containers are available.
        conf.setInt("yarn.nodemanager.resource.cpu-vcores", numContainers);
        conf.setInt("yarn.scheduler.maximum-allocation-vcores", 1);
        
        final int taskMemory = 200;
        final int containerMemory = taskMemory * 3 / 2;
        
        conf.setInt("mapreduce.map.memory.mb", containerMemory);
        conf.setInt("mapreduce.reduce.memory.mb", containerMemory);
        conf.setInt("yarn.scheduler.minimum-allocation-mb", taskMemory);
        conf.setInt("yarn.scheduler.maximum-allocation-mb", taskMemory);
        conf.setInt("yarn.nodemanager.resource.memory-mb", containerMemory * numContainers);
        conf.setInt("yarn.app.mapreduce.am.resource.mb", containerMemory);
        conf.set("mapred.child.java.opts", "-Xmx" + taskMemory + "m");

        // Tell the minicluster where to put data. Sadly, this doesn't work.
        // See comment in shutdown()
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dfsDir.getAbsolutePath());
        
        Iterator<Entry<String, String>> iter = conf.iterator();
        while (iter.hasNext()) {
            Entry<String, String> prop = iter.next();
            System.out.println(prop.getKey() + ": " + prop.getValue());
        }
        
        _dfs = new MiniDFSCluster.Builder(conf).build();
        _dfs.waitClusterUp();
        
        YarnConfiguration clusterConf = new YarnConfiguration();
        clusterConf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
        clusterConf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
        _yarn = new MiniYARNCluster("cluster1", numContainers, 1, 1);
        _yarn.init(clusterConf);
        _yarn.start();

        // Update _conf to match what we get back from the minicluster
        Configuration newConf = _yarn.getConfig();
        _conf = new JobConf(newConf);
    }

    public void shutdown() throws InterruptedException, IOException {
        if (_yarn != null) {
            _yarn.stop();
        }

        if (_dfs != null) {
            _dfs.shutdown();
            // Note that we don't wait for the cluster to be down, since
            // isClusterUp() always returns true.
        }
        
        // Sadly, the Hadoop 2.2 MiniDFSCluster always put stuff into a "target"
        // directory that's relative to the CWD. So we'll need to get rid of "target/MR_IDENTIFIER"
        // and also target/test-dir (hard-coded as well)
        File targetDir = new File("target");
        File idDir = new File(targetDir, MR_IDENTIFIER);
        FileUtils.deleteDirectory(idDir);
        
        File testDir = new File(targetDir, "test-dir");
        FileUtils.deleteDirectory(testDir);
        
        // And now, if "target" is empty, we should get rid of it to, since we created it.
        if (FileUtils.listFiles(targetDir, null, false).isEmpty()) {
            FileUtils.deleteDirectory(targetDir);
        }
    }

}
