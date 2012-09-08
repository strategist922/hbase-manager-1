package com.wandoujia.hbase.manager.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;

public class testHBaseClient extends TestCase {
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

    public void testListTables() throws IOException {
        HBaseClient client = new HBaseClient();
        HTableDescriptor tables[] = client.listTables();
        for (HTableDescriptor table: tables) {
            System.out.println(table.toString());
        }
    }

    public void testInsert() throws IOException {
        HBaseClient client = new HBaseClient();
        Map<String, byte[]> values = new HashMap<String, byte[]>();
        values.put("c1", "v1".getBytes());
        client.insert(defaultRowKey, values);
    }

    public void testGetRow() throws IOException {
        HBaseClient client = new HBaseClient(defaultTableName,
                defaultFamily.getBytes());
        Map<String, byte[]> values = new HashMap<String, byte[]>();
        values.put("c1", "v1".getBytes());
        values.put("c2", "v2".getBytes());
        values.put("c3", "v3".getBytes());
        client.insert(defaultRowKey, values);
        byte[] result = client.getRow(defaultRowKey, "c1".getBytes(), false);
        System.out.println(new String(result));

        Set<byte[]> columns = new HashSet<byte[]>();
        columns.add("c2".getBytes());
        columns.add("c3".getBytes());
        Map<String, byte[]> results = client.getRow(defaultRowKey, columns,
                false);
        for (Map.Entry<String, byte[]> entry: results.entrySet()) {
            System.out.println(entry.getKey() + ": "
                    + new String(entry.getValue()));
        }
    }

    private void printAndCloseScanner(ResultScanner scanner) throws IOException {
        try {
            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                System.out.println(new String(rr.getRow()));
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    public void testGetScanner() throws IOException {
        HBaseClient client = new HBaseClient(defaultTableName,
                defaultFamily.getBytes());
        for (int i = 0; i < 10; i++) {
            Map<String, byte[]> values = new HashMap<String, byte[]>();
            values.put("c1", ("v" + i).getBytes());
            client.insert(String.valueOf(i), values);
        }

        System.out.println("scanner without filter");
        ResultScanner scanner = client.getScanner("0", "999", 10, false);
        printAndCloseScanner(scanner);

        System.out.println("scanner with filter, filter missing");
        Filter filter = HBaseUtil.getFilter(defaultFamily.getBytes(),
                "c1".getBytes(), CompareOp.EQUAL, "v2".getBytes(), true);
        scanner = client.getScanner("0", "999", filter, 10, false);
        printAndCloseScanner(scanner);

        System.out.println("scanner with filter, not filter missing");
        filter = HBaseUtil.getFilter(defaultFamily.getBytes(),
                "c1111".getBytes(), CompareOp.EQUAL, "v2".getBytes(), true);
        scanner = client.getScanner("0", "999", filter, 10, false);
        printAndCloseScanner(scanner);
    }
}
