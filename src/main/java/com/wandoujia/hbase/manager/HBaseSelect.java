package com.wandoujia.hbase.manager;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import com.wandoujia.hbase.manager.util.ParaUtil;

public class HBaseSelect {

    public static final String SEPERATOR = "\t";

    public static void usage() {
        System.err
                .println("HBaseSelect <table> <start_row> <stop_row> <expressions>");
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
        HBaseBuilder hb = new HBaseBuilder();
        hb.setTableName(tableName);

        List<Map<String, String>> results = hb.selectByFilter(startRow,
                stopRow, filters, opers);
        long consumes = System.currentTimeMillis() - start;

        String header = "";
        if (results.size() > 0) {
            Map<String, String> record = results.get(0);
            for (Map.Entry<String, String> entry: record.entrySet()) {
                header += entry.getKey() + SEPERATOR;
            }
        }
        System.out.println(header);
        for (Map<String, String> record: results) {
            String line = "";
            for (Map.Entry<String, String> entry: record.entrySet()) {
                line += entry.getValue() + SEPERATOR;
            }
            System.out.println(line);
        }

        System.out.println("Find Rows: " + results.size());
        System.out.println("Use Time(s): " + consumes / 1000f);

    }
}
