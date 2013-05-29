package com.scaleunlimited.cascading;

import java.util.ArrayList;

import junit.framework.Assert;

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
public class UniqueCountTest extends Assert {

    private static final String OUTPUT_DIR = "build/test/UniqueCountTest";
    private static final Fields IN_FIELDS = new Fields("user", "id", "value");
    private static final Fields COUNT_FIELD = new Fields("count");
    private static final Fields OUT_FIELDS = new Fields("user", "count");

//    Using a very small threshold, to verify results when map-side uniqueness has to flush.
    
    
    @Test
    public void testSingleFields() throws Exception {
        final Fields groupFields = new Fields("user");
        LocalPlatform platform = new LocalPlatform(UniqueCountTest.class);
        Flow flow = makeFlow("testSingleFields", 10, groupFields, new Fields("id"),  false, platform);
        flow.complete();
        
        // validate
        int[] counts = getUniqueCounts(platform, "testSingleFields", groupFields, "user-0", 4);
        assertEquals(1, counts.length);
        assertEquals(2, counts[0]);
        
        // Also check that we don't get nulls for the id field
        BasePath outputDir = platform.makePath(OUTPUT_DIR);
        BasePath testDir = platform.makePath(outputDir, "testSingleFields");
        BasePath dataPath = platform.makePath(testDir, "out");

        Tap tap = platform.makeTap(platform.makeBinaryScheme(OUT_FIELDS), dataPath);

        TupleEntryIterator iter = tap.openForRead(platform.makeFlowProcess());
        while (iter.hasNext()) {
            TupleEntry next = iter.next();
            assertFalse(next.getFields().contains(new Fields("id")));
        }
        
        iter.close();
    }
    
    @Test
    public void testMultipleGroupFields() throws Exception {
        final Fields groupFields = new Fields("user", "id");
        LocalPlatform platform = new LocalPlatform(UniqueCountTest.class);
        Flow flow = makeFlow("testMultipleGroupFields", 10, groupFields, new Fields("value"), false, platform);
        flow.complete();
        
        // validate
        int[] counts = getUniqueCounts(platform, "testMultipleGroupFields", groupFields, "user-6", 7);

        assertEquals(2, counts.length);
        assertEquals(2, counts[0]);
        assertEquals(1, counts[1]);
    }

    @Test
    public void testMultipleUniqueFields() throws Exception {
        final Fields groupFields = new Fields("user");

        LocalPlatform platform = new LocalPlatform(UniqueCountTest.class);
        Flow flow = makeFlow("testMultipleUniqueFields", 10, groupFields, new Fields("id", "value"), false, platform);
        flow.complete();
        
        // I should get a total of four records, one for each user we wind up creating (0, 3, 6, 9)
        int[] counts = getUniqueCounts(platform, "testMultipleUniqueFields", groupFields, "user-6", 4);

        // I should get one entry for "user-6", with the three unique combinations of "id" and "value".
        assertEquals(1, counts.length);
        assertEquals(3, counts[0]);
    }

    @Test  
    public void testNullUniqueFieldValue() throws Exception {
        final Fields groupFields = new Fields("user");

        LocalPlatform platform = new LocalPlatform(UniqueCountTest.class);
        Flow flow = makeFlow("testNullUniqueFieldValue", 10, groupFields, new Fields("id"), true, platform);
        flow.complete();
        int[] counts = getUniqueCounts(platform, "testNullUniqueFieldValue", groupFields, "user-6", 4);
        assertEquals(1, counts.length);
        assertEquals(1, counts[0]);
    }

    
    @Test
    public void testHadoopCluster() throws Exception {
        final Fields groupFields = new Fields("user");
        
        MiniClusterPlatform platform = new MiniClusterPlatform(UniqueCountTest.class, 
                       2, 2, OUTPUT_DIR+"/testHadoopCluster/log/", OUTPUT_DIR+"/testHadoopCluster/tmp");
        Flow flow = makeFlow("testHadoopCluster", 10, groupFields, new Fields("id"),  false, platform);
        flow.complete();
        // validate
        int[] counts = getUniqueCounts(platform, "testHadoopCluster", groupFields, "user-0", 4);
        assertEquals(1, counts.length);
        assertEquals(2, counts[0]);
    }

    @SuppressWarnings({"unchecked" })
    private Flow makeFlow(String testName, int numDatums,  
                    Fields groupFields, Fields uniqueFields,
                    boolean insertNullIdField,
                    BasePlatform platform) throws Exception {
        
        BasePath outputDir = platform.makePath(OUTPUT_DIR);
        BasePath testDir = platform.makePath(outputDir, testName);
        BasePath in = platform.makePath(testDir, "in");
        Tap sourceTap = platform.makeTap(platform.makeBinaryScheme(IN_FIELDS), in, SinkMode.REPLACE);
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
                    write.add(new Tuple(username, null, i));
                } else {
                    Tuple t = new Tuple(username, i % 2, i);
                    write.add(t);
                }
                i++;
                j++;
            }
        }
        
        write.close();

        Pipe pipe = new Pipe("test");
        UniqueCount assembly = new UniqueCount(pipe, groupFields, uniqueFields, COUNT_FIELD, 2);

        Pipe uniqueCountsPipe = assembly.getTailPipe();
        
        BasePath out = platform.makePath(testDir, "out");
        Tap sinkTap = platform.makeTap(platform.makeBinaryScheme(groupFields.append(COUNT_FIELD)), out, SinkMode.REPLACE);

        Flow flow = platform.makeFlowConnector().connect(testName, sourceTap, sinkTap, uniqueCountsPipe);
        FlowUtils.nameFlowSteps(flow);
        return flow;
    }

    
    @SuppressWarnings("unchecked")
    private int[] getUniqueCounts(BasePlatform platform, String testName, Fields groupFields,
                    String user, int total) throws  Exception {
        
        ArrayList<Integer> uniqueCountsList = new ArrayList<Integer>();
        
        BasePath outputDir = platform.makePath(OUTPUT_DIR);
        BasePath testDir = platform.makePath(outputDir, testName);
        BasePath dataPath = platform.makePath(testDir, "out");

        Tap tap = platform.makeTap(platform.makeBinaryScheme(groupFields.append(COUNT_FIELD)), dataPath);

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
