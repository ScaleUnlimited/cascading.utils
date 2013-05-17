package com.scaleunlimited.cascading;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.planner.PlannerException;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.hadoop.test.MiniClusterPlatform;
import com.scaleunlimited.cascading.local.LocalPlatform;


@SuppressWarnings("rawtypes")
public class UniqueCountTest {

    private static final String OUTPUT_DIR = "build/test/UniqueCountTest";
    private static final Fields FIELDS = new Fields("user", "id", "value", "count");

//    Using a very small threshold, to verify results when map-side uniqueness has to flush.
    
    
    @Test
    public void testSingleFields() throws Exception {
        LocalPlatform platform = new LocalPlatform(UniqueCountTest.class);
        Flow flow = makeFlow("testSingleFields", 10, new Fields("user"), new Fields("id"), new Fields("count"),  false, platform);
        flow.complete();
        // validate
        int[] counts = getUniqueCounts(platform, "testSingleFields", "user-0", 4);
        assertEquals(1, counts.length);
        assertEquals(2, counts[0]);
    }
    
    @Test
    public void testMultipleGroupFields() throws Exception {
        LocalPlatform platform = new LocalPlatform(UniqueCountTest.class);
        Flow flow = makeFlow("testMultipleGroupFields", 10, new Fields("user", "id"), new Fields("value"), new Fields("count"), false, platform);
        flow.complete();
        // validate
        
        int[] counts = getUniqueCounts(platform, "testMultipleGroupFields", "user-6", 7);

        assertEquals(2, counts.length);
        assertEquals(2, counts[0]);
        assertEquals(1, counts[1]);
    }

    @Test (expected = PlannerException.class)  
    public void testMultipleUniqueFields() throws Exception {
        LocalPlatform platform = new LocalPlatform(UniqueCountTest.class);
        Flow flow = makeFlow("testMultipleUniqueFields", 10, new Fields("user"), new Fields("id", "value"), new Fields("count"), false, platform);
        flow.complete();
    }

    @Test  
    public void testNullUniqueFieldValue() throws Exception {
        LocalPlatform platform = new LocalPlatform(UniqueCountTest.class);
        Flow flow = makeFlow("testNullUniqueFieldValue", 10, new Fields("user"), new Fields("id"), new Fields("count"), true, platform);
        flow.complete();
        int[] counts = getUniqueCounts(platform, "testNullUniqueFieldValue", "user-6", 4);
        assertEquals(1, counts.length);
        assertEquals(1, counts[0]);
    }

    
    @Test
    public void testHadoopCluster() throws Exception {
        
        MiniClusterPlatform platform = new MiniClusterPlatform(UniqueCountTest.class, 
                       2, 2, OUTPUT_DIR+"/testHadoopCluster/log/", OUTPUT_DIR+"/testHadoopCluster/tmp");
        Flow flow = makeFlow("testHadoopCluster", 10, new Fields("user"), new Fields("id"), new Fields("count"),  false, platform);
        flow.complete();
        // validate
        int[] counts = getUniqueCounts(platform, "testHadoopCluster", "user-0", 4);
        assertEquals(1, counts.length);
        assertEquals(2, counts[0]);
    }

    @SuppressWarnings({"unchecked" })
    private Flow makeFlow(String testName, int numDatums,  
                    Fields groupFields, Fields uniqueFields, Fields countField,
                    boolean insertNullIdField,
                    BasePlatform platform) throws Exception {
        
        BasePath outputDir = platform.makePath(OUTPUT_DIR);
        BasePath testDir = platform.makePath(outputDir, testName);
        BasePath in = platform.makePath(testDir, "in");
        Tap sourceTap = platform.makeTap(platform.makeBinaryScheme(FIELDS), in, SinkMode.REPLACE);
        TupleEntryCollector write = sourceTap.openForWrite(platform.makeFlowProcess());
        
        int i = 0;
        while (i < numDatums) {
            
            // have user x 3, id x 2 and value x 1
            String username = "user-" + i;
            int j = 0;
            while (j < 3) {
                if (i >= numDatums) {
                    break;
                }
                if (insertNullIdField) {
                    write.add(new Tuple(username, null, i, null));
                } else {
                    write.add(new Tuple(username, i % 2, i, null));
                }
                i++;
                j++;
            }
        }
        
        write.close();

        Pipe pipe = new Pipe("test");
        UniqueCount assembly = new UniqueCount(pipe, groupFields, uniqueFields, countField, 2);

        Pipe uniqueCountsPipe = assembly.getTailPipe();
        
        BasePath out = platform.makePath(testDir, "out");
        Tap sinkTap = platform.makeTap(platform.makeBinaryScheme(FIELDS), out, SinkMode.REPLACE);

        Flow flow = platform.makeFlowConnector().connect(testName, sourceTap, sinkTap, uniqueCountsPipe);
        FlowUtils.nameFlowSteps(flow);
        return flow;
    }

    
    @SuppressWarnings("unchecked")
    private int[] getUniqueCounts(BasePlatform platform, String testName,
                    String user, int total) throws  Exception {
        
        ArrayList<Integer> uniqueCountsList = new ArrayList<Integer>();
        
        BasePath outputDir = platform.makePath(OUTPUT_DIR);
        BasePath testDir = platform.makePath(outputDir, testName);
        BasePath dataPath = platform.makePath(testDir, "out");

        Tap tap = platform.makeTap(platform.makeBinaryScheme(FIELDS), dataPath);

        TupleEntryIterator iter = tap.openForRead(platform.makeFlowProcess());
        int num = 0;
        while (iter.hasNext()) {
            TupleEntry next = iter.next();
            if (next.getString("user").equals(user)) {
                uniqueCountsList.add(next.getInteger("count"));
            }
            num++;
        }
        iter.close();
        assertEquals("Total number of records", total, num);
        return ArrayUtils.toPrimitive(uniqueCountsList.toArray(new Integer[uniqueCountsList.size()]));
    }
}
