package com.wandoujia.hbase.manager;

import java.io.IOException;
import org.apache.hadoop.hbase.HTableDescriptor;

public class HBaseOperate {

    public static void usage() {
        System.err.println("HBaseOperate <op-type>");
        System.err
                .println("op-type:\t\texample: list, schema, drop, disable, enable");
        System.exit(1);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            usage();
        }

        String opType = args[0];
        HBaseBuilder hb = new HBaseBuilder();

        long start = System.currentTimeMillis();
        if (opType.equals("list")) {
            HTableDescriptor[] tables = hb.listTables();
            for (HTableDescriptor table: tables) {
                System.out.println(table.getNameAsString());
            }
        } else if (opType.equals("schema")) {
            System.out.println(hb.showSchema(args[1]).toString());
        } else if (opType.equals("drop")) {
            hb.drop(args[1]);
        } else if (opType.equals("disable")) {
            hb.disable(args[1]);
        } else if (opType.equals("enable")) {
            hb.enable(args[1]);
        }

        long consumes = System.currentTimeMillis() - start;
        System.out.println("Use Time(s): " + consumes / 1000f);
    }
}
