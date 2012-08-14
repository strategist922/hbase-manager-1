package com.wandoujia.hbase.manager;

import java.io.IOException;

import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;

public class CreateTablePreSplit {
    String tableName = "";

    String familyName = "";

    int blockSize = 0;

    String startKey = "";

    String endKey = "";

    int numRegions = 0;

    int force = 0;

    HBaseClient hb = null;

    public CreateTablePreSplit(String tableName, String familyName,
            int blockSize, String startKey, String endKey, int numRegions,
            int force) {
        this.tableName = tableName;
        this.familyName = familyName;
        this.blockSize = blockSize;
        this.startKey = startKey;
        this.endKey = endKey;
        this.numRegions = numRegions;
        this.force = force;

        hb = new HBaseClient();
    }

    public void create() throws IOException {
        if (this.hb.getAdmin().tableExists(this.tableName)) {
            if (this.force == 0) {
                System.out
                        .println(String.format(
                                "Table %s already exist, please check",
                                this.tableName));
                return;
            } else {
                System.out.println(String.format(
                        "Drop table %s before create it", this.tableName));
                this.hb.drop(this.tableName);
            }
        } else {
            System.out.println(String.format("Table %s does not exist",
                    this.tableName));
        }
        byte[][] splits = null;
        splits = HBaseClient.getHexSplits(this.startKey, this.endKey,
                this.numRegions);
        this.hb.create(this.tableName, this.familyName, BloomType.ROW,
                Algorithm.NONE, false, true, this.blockSize, 1, splits);

    }

    public static void usage() {
        System.err
                .println("CreateTableIfNotExist <tableName> <familyName> <blockSize> <startKey> <endKey> <numRegions> [force]");
        System.err.println("table:\t\texample: android_app_download");
        System.err.println("cf:\t\texample: c");
        System.err.println("block_size:\t\texample: 65536");
        System.err.println("start_key:\t\texample: 0000000000000000");
        System.err.println("end_key:\t\texample: ffffffffffffffff");
        System.err.println("num_regions:\t\texample: 10");
        System.err.println("force:\t\texample: default 0");
        System.exit(1);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 6) {
            usage();
        }

        String tableName = args[0];
        String familyName = args[1];
        int blockSize = Integer.parseInt(args[2]);
        String startKey = args[3];
        String endKey = args[4];
        int numRegions = Integer.parseInt(args[5]);
        int force = 0;
        if (args.length == 7) {
            force = Integer.parseInt(args[6]);
        }

        CreateTablePreSplit ctps = new CreateTablePreSplit(tableName,
                familyName, blockSize, startKey, endKey, numRegions, force);
        ctps.create();

    }
}
