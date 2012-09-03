package com.wandoujia.hbase.load.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;

import com.wandoujia.hbase.manager.HBaseClient;
import com.wandoujia.hbase.manager.HBaseMultiThreadClient;

public class HBaseLoadTest {

    public static String tableName = "load_test";

    public static String familyName = "c";

    public static String columnName = "data";

    public static int blockSize = 65536;

    private static Random random = new Random();

    private HBaseMultiThreadClient hbaseClient;

    public static String[] BYTES = {
        "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d",
        "e", "f"
    };

    // properties
    private boolean cacheBlocks = true;

    private int scanRowNum = 1;

    public HBaseLoadTest(boolean cacheBlocks, int scanRowNum)
            throws IOException {
        this.cacheBlocks = cacheBlocks;
        this.scanRowNum = scanRowNum;
        HBaseLoadTest.prepare();
        hbaseClient = new HBaseMultiThreadClient(10);
    }

    public HBaseLoadTest(String tableName, String familyName,
            String columnName, boolean cacheBlocks, int scanRowNum)
            throws IOException {
        HBaseLoadTest.tableName = tableName;
        HBaseLoadTest.familyName = familyName;
        HBaseLoadTest.columnName = columnName;
        this.cacheBlocks = cacheBlocks;
        this.scanRowNum = scanRowNum;
        HBaseLoadTest.prepare();
        hbaseClient = new HBaseMultiThreadClient(10);
    }

    public void close() throws IOException {
        if (hbaseClient != null) {
            hbaseClient.close();
        }
    }

    public void put() throws IOException {
        Map<String, byte[]> values = new HashMap<String, byte[]>();
        byte[] bytes = new byte[1024];
        random.nextBytes(bytes);
        values.put(columnName, bytes);
        String rowKey = UUID.randomUUID().toString();
        hbaseClient.insert(tableName, rowKey, familyName, values);
    }

    public void scan() throws IOException {
        ResultScanner scanner = hbaseClient.getScanner(tableName,
                randomStartRow(7), cacheBlocks);
        try {
            int count = 0;
            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                count++;
                if (count >= scanRowNum) {
                    break;
                }
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    public static String randomStartRow(int length) {
        String randomString = "";
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(BYTES.length);
            randomString += BYTES[index];
        }
        return randomString;
    }

    public static void prepare() throws IOException {
        HBaseClient hb = new HBaseClient();
        hb.create(tableName, familyName, BloomType.ROW, Algorithm.NONE, false,
                true, blockSize, 1, null);
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 6) {
            System.err
                    .println("HBaseLoadTest [tableName] [familyName] [columnName] [cacheBlocks] [scanRowNum] [runTimes]");
            System.exit(1);
        }
        String tableName = args[0];
        String familyName = args[1];
        String columnName = args[2];
        boolean cacheBlocks = Boolean.parseBoolean(args[3]);
        int scanRowNum = Integer.parseInt(args[4]);
        int runTimes = Integer.parseInt(args[5]);
        HBaseLoadTest test = new HBaseLoadTest(tableName, familyName,
                columnName, cacheBlocks, scanRowNum);
        for (int i = 0; i < runTimes; i++) {
            long start = System.currentTimeMillis();
            test.scan();
            System.out.println("use time: "
                    + (System.currentTimeMillis() - start));
        }
    }

}
