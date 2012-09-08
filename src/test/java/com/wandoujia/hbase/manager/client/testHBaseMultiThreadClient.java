package com.wandoujia.hbase.manager.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;

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
        System.out.println("create table");
        createTable();
    }

    @Override
    protected void tearDown() throws Exception {
        System.out.println("drop table");
        dropTable();
    }

    private void createTable() throws IOException {
        HBaseClient client = new HBaseClient();
        client.createHTable(defaultTableName, defaultFamily.getBytes(),
                BloomType.NONE, Algorithm.NONE, false, false, 65536, 1, null);
    }

    private void dropTable() throws IOException {
        HBaseClient client = new HBaseClient();
        client.drop(defaultTableName);
    }

    public void testInsertRow() throws IOException {
        HBaseMultiThreadClient client = new HBaseMultiThreadClient();
        Map<String, byte[]> values = new HashMap<String, byte[]>();
        values.put("c1", "v1".getBytes());
        client.insert(defaultTableName, defaultFamily.getBytes(),
                defaultRowKey, values);
    }

    public void testGetRow() throws IOException {
        HBaseMultiThreadClient client = new HBaseMultiThreadClient(100, 65536);
        Map<String, byte[]> values = new HashMap<String, byte[]>();
        values.put("c1", "v1".getBytes());
        values.put("c2", "v2".getBytes());
        values.put("c3", "v3".getBytes());
        client.insert(defaultTableName, defaultFamily.getBytes(),
                defaultRowKey, values);
        client.flush(defaultTableName);

        byte[] result = client
                .getRow(defaultTableName, defaultFamily.getBytes(),
                        defaultRowKey, "c1".getBytes(), false);
        System.out.println(new String(result));

        Set<byte[]> columns = new HashSet<byte[]>();
        columns.add("c2".getBytes());
        columns.add("c3".getBytes());
        Map<String, byte[]> results = client.getRow(defaultTableName,
                defaultFamily.getBytes(), defaultRowKey, columns, false);
        for (Map.Entry<String, byte[]> entry: results.entrySet()) {
            System.out.println(entry.getKey() + ": "
                    + new String(entry.getValue()));
        }
    }
}
