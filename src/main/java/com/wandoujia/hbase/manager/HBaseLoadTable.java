package com.wandoujia.hbase.manager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseLoadTable {

    public static String HBASE_ROW_KEY = "row_key";

    public static String FAMILY_NAME = "c";

    public static long BATCH_SIZE = 10000;

    public static int BLOCK_SIZE = 65536;

    public static long writeBufferSize = 1048576;

    public static void usage() {
        System.err.println("HBaseLoadTable <table> <qualifiers> <local_file>");
        System.err.println("table:\t\texample: android_app_download");
        System.err.println("qualifiers:\t\texample: row_key,vc,vn");
        System.err.println("local_file:\t\texample: /path/file");
        System.exit(1);
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            usage();
        }

        String tableName = args[0];
        String qualifiers[] = args[1].split(",");
        String localFile = args[2];
        byte[][] splits = null;

        HBaseClient hb = new HBaseClient();
        HTableDescriptor desc = hb.create(tableName, FAMILY_NAME,
                BloomType.ROW, Algorithm.NONE, false, true, BLOCK_SIZE, 1,
                splits);
        if (desc == null) {
            System.out.println("Table: " + tableName + " already exists.");
        }
        hb.setTableName(tableName);
        hb.setWriteBufferSize(writeBufferSize);

        BufferedReader reader = new BufferedReader(new FileReader(localFile));
        String line = null;
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split("\t");
            String rowKey = fields[0];
            Map<String, byte[]> values = new HashMap<String, byte[]>();
            for (int i = 1; i < qualifiers.length; i++) {
                if (fields.length > i) {
                    values.put(qualifiers[i], Bytes.toBytes(fields[i]));
                } else {
                    values.put(qualifiers[i], Bytes.toBytes(""));
                }
            }
            hb.insert(rowKey, FAMILY_NAME, values);
        }
        hb.getTable().flushCommits();
        hb.getTable().close();
    }
}
