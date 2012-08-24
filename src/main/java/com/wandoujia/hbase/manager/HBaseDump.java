package com.wandoujia.hbase.manager;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import com.wandoujia.hbase.manager.util.ParaUtil;

public class HBaseDump {

    public static void usage() {
        System.err
                .println("HBaseDump <table> <startRow> <stopRow> <expressions> <qualifiers> <output_file>");
        System.err.println("table:\t\texample: android_app_download");
        System.err.println("startRow:\t\texample: hour_20120420");
        System.err.println("stopRow:\t\texample: hour_20120421");
        System.err
                .println("expressions:\t\texample: field1:==:value1,field2:!=:value2");
        System.err.println("\t\t\tno expressions: -");
        System.err.println("qualifiers:\t\texample: row_key,vc,vn");
        System.err.println("output_file:\t\texample: /path/file");
        System.exit(1);
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 6) {
            usage();
        }

        String tableName = args[0];
        String startRow = args[1];
        String stopRow = args[2];
        String expressions = args[3];
        String qualifiers = args[4];
        String outputFilePath = args[5];

        Map<String, String> filters = new HashMap<String, String>();
        Map<String, CompareOp> opers = new HashMap<String, CompareOp>();

        ParaUtil.parseHBaseExpressions(expressions, filters, opers);

        System.out.println("filters: " + filters.toString());
        System.out.println("opers: " + opers.toString());

        HBaseClient hb = new HBaseClient();
        hb.setTableName(tableName);

        BufferedWriter writer = new BufferedWriter(new FileWriter(
                outputFilePath, false), 40960);

        long start = System.currentTimeMillis();
        long rows = hb.dumpByFilter(startRow, stopRow, filters, opers,
                qualifiers.split(","), writer, false);
        long consumes = System.currentTimeMillis() - start;
        System.out.println("Dump Rows: " + rows);
        System.out.println("Use Time(s): " + consumes / 1000f);

    }
}
