package com.wandoujia.hbase.manager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import com.wandoujia.hbase.manager.util.ParaUtil;

public class HBaseDelete {

    public static void usage() {
        System.err
                .println("HBaseDelete <table> <startRow> <stopRow> <expressions>");
        System.err.println("table:\t\texample: android_app_download");
        System.err.println("startRow:\t\texample: hour_20120420");
        System.err.println("stopRow:\t\texample: hour_20120421");
        System.err
                .println("expressions:\t\texample: field1:==:value1,field2:!=:value2");
        System.err.println("\t\t\tno expressions: -");
        System.exit(1);
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            usage();
        }

        String tableName = args[0];
        String startRow = args[1];
        String stopRow = args[2];
        String expressions = args[3];

        Map<String, String> filters = new HashMap<String, String>();
        Map<String, CompareOp> opers = new HashMap<String, CompareOp>();

        ParaUtil.parseHBaseExpressions(expressions, filters, opers);

        System.out.println("filters: " + filters.toString());
        System.out.println("opers: " + opers.toString());

        long start = System.currentTimeMillis();
        HBaseClient hb = new HBaseClient();
        hb.setTableName(tableName);
        long affectRows = hb.deleteByFilter(startRow, stopRow, filters, opers,
                false);
        long consumes = System.currentTimeMillis() - start;

        System.out.println("Affect Rows: " + affectRows);
        System.out.println("Use Time(s): " + consumes / 1000f);

    }
}
