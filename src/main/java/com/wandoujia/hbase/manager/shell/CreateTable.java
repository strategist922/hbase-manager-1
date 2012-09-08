package com.wandoujia.hbase.manager.shell;

import java.io.IOException;

import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;

import com.wandoujia.hbase.manager.client.HBaseClient;
import com.wandoujia.hbase.manager.client.HBaseUtil;

/**
 * @author fengzanfeng
 */
public class CreateTable {
    private String tableName;

    private byte[] familyName = HBaseUtil.defaultFamilyName;

    private int blockSize = 65536;

    private int maxVersions = 1;

    private String startKey;

    private String endKey;

    private int numRegions = 1;

    byte[][] splits;

    private HBaseClient client = new HBaseClient();

    public CreateTable() throws IOException {

    }

    public void create() throws IOException {
        if (numRegions > 1) {
            splits = HBaseUtil.getHexSplits(startKey, endKey, numRegions);
        }
        client.createHTable(tableName, familyName, BloomType.ROW,
                Algorithm.NONE, false, true, blockSize, maxVersions, splits);
    }

    public static void usage() {
        System.err
                .println("CreateTable <table name> [family name] [block size] [num of version] [start key] [end key] [num of region]");
        System.err.println("<table name>\t\t\thbase table name");
        System.err.println("[family name]\t\t\tdefault: 'c'");
        System.err.println("[block size]\t\t\tdefault: 65536");
        System.err.println("[start key]\t\t\tused for pre-create regions");
        System.err.println("[end key]\t\t\tused for pre-create regions");
        System.err.println("[region number]\t\t\tused for pre-create regions");
        System.exit(-1);
    }

    private void parseArgs(String[] args) {
        if (args.length < 1) {
            usage();
        }
        this.tableName = args[0];

        if (args.length >= 2) {
            this.familyName = args[1].getBytes();
        }
        if (args.length >= 3) {
            this.blockSize = Integer.parseInt(args[2]);
        }
        if (args.length >= 4) {
            this.maxVersions = Integer.parseInt(args[3]);
        }
        if (args.length >= 7) {
            this.startKey = args[4];
            this.endKey = args[5];
            this.numRegions = Integer.parseInt(args[6]);
        }
    }

    public static void main(String[] args) {
        try {
            CreateTable instance = new CreateTable();
            instance.parseArgs(args);
            instance.create();
        } catch (IOException e) {
            System.err.println("Create table failed.");
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Create table failed.");
            e.printStackTrace();
        }
    }
}
