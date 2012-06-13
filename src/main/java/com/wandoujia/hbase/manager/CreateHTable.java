package com.wandoujia.hbase.manager;

import java.io.IOException;

import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;

public class CreateHTable {
    public static void usage() {
        System.err
                .println("CreateHTable <tableName> <familyName> <blockSize> [startKey] [endKey] [numRegions]");
        System.exit(1);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            usage();
        }

        String tableName = args[0];
        String familyName = args[1];
        int blockSize = Integer.parseInt(args[2]);

        byte[][] splits = null;
        if (args.length == 6) {
            splits = HBaseBuilder.getHexSplits(args[3], args[4],
                    Integer.parseInt(args[5]));
        }
        
        HBaseBuilder hb = new HBaseBuilder();
        hb.create(tableName, familyName, BloomType.ROW, Algorithm.NONE, false,
                true, blockSize, 1, splits);
    }
}
