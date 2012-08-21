package com.wandoujia.hbase.manager;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.TestCase;

public class testHBaseMultiThreadClient extends TestCase {
    public static final String defaultTableName = "unit_test_table";

    public static final String defaultRowKey = "key_001";

    public static final String defaultFamily = "content";

    public static final String defaultColumn = "source";

    public static final Set<String> defaultColumns = new HashSet<String>();

    @Override
    protected void setUp() throws Exception {
        defaultColumns.add("source");
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testGet1() throws IOException {
        HBaseMultiThreadClient client = new HBaseMultiThreadClient();
        Map<String, byte[]> row = client.get(defaultTableName, defaultRowKey,
                defaultFamily, defaultColumns);
        if (row.get("source") != null) {
            System.out.println(new String(row.get("source")));
        }

    }

    public void testGet2() throws IOException {
        HBaseMultiThreadClient client = new HBaseMultiThreadClient(10, 10240);
        Map<String, byte[]> row = client.get(defaultTableName, defaultRowKey,
                defaultFamily, defaultColumns);
        if (row.get(defaultColumn) != null) {
            System.out.println(new String(row.get(defaultColumn)));
        }
    }

    public void testInsert() throws IOException {
        Map<String, byte[]> values = new HashMap<String, byte[]>();
        values.put(defaultColumn, Bytes.toBytes("value_001"));
        HBaseMultiThreadClient client = new HBaseMultiThreadClient(10, 10240);
        client.insert(defaultTableName, defaultRowKey, defaultFamily, values);
        client.close();
    }

}
