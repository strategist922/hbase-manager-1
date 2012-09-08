package com.wandoujia.hbase.manager.shell;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList.Operator;

import com.wandoujia.hbase.manager.client.HBaseClient;
import com.wandoujia.hbase.manager.client.HBaseUtil;
import com.wandoujia.hbase.manager.util.Condition;
import com.wandoujia.hbase.manager.util.SQL;
import com.wandoujia.hbase.manager.util.HBaseSQLParser;

/**
 * @author fengzanfeng
 */
public class HBaseQuery {
    private String tableName;

    private byte[] familyName;

    private String startRow;

    private String stopRow;

    private SQL sql;

    private boolean filterIfMissing;

    private BufferedWriter writer;

    private HBaseClient client;

    private long counter = 0;

    public HBaseQuery() throws IOException {

    }

    public void query() throws IOException {
        System.out.println(sql.toString());
        client = new HBaseClient(this.tableName, this.familyName);
        List<byte[]> whereColumns = new ArrayList<byte[]>();
        List<CompareOp> whereCompareOps = new ArrayList<CompareOp>();
        List<byte[]> whereValues = new ArrayList<byte[]>();
        Operator operator = sql.getOperator();
        Filter filter = null;
        if (sql.getConditions() != null) {
            for (Condition condition: this.sql.getConditions()) {
                whereColumns.add(condition.getField().getBytes());
                whereCompareOps.add(Condition.getHBaseCompareOp(condition
                        .getSqlCompareOp()));
                whereValues.add(condition.getValue().getBytes());
            }
            filter = HBaseUtil.getFilter(familyName, whereColumns,
                    whereCompareOps, whereValues, operator, filterIfMissing);
        }
        ResultScanner scanner = client.getScanner(startRow, stopRow, filter,
                1000, false);
        try {
            for (Result result = scanner.next(); result != null; result = scanner
                    .next()) {
                counter++;
                Map<String, byte[]> rowColumns = HBaseUtil
                        .getRowColumns(result);
                output(rowColumns);
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    private void write(String line) throws IOException {
        if (writer != null) {
            writer.write(line + "\n");
        } else {
            System.out.println(line);
        }

    }

    private void output(Map<String, byte[]> rowColumns) throws IOException {
        StringBuilder sb = new StringBuilder();
        if (sql.getFields() == null) {
            // print all columns
            for (Map.Entry<String, byte[]> entry: rowColumns.entrySet()) {
                sb.append(entry.getKey() + ":" + new String(entry.getValue())
                        + "\t");
            }
            sb.replace(sb.length() - 1, sb.length(), "");
        } else {
            // print columns of specified
            for (String field: sql.getFields()) {
                sb.append(new String(rowColumns.get(field)) + "\t");
            }
            sb.replace(sb.length() - 1, sb.length(), "");
        }
        write(sb.toString());
    }

    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    public static void usage() {
        System.err
                .println("HBaseQuery <family> <startRow> <stopRow> <SQL> [filterIfMissing] [output]");
        System.err.println("<family>             family name");
        System.err
                .println("[startRow]           scan start from the start key");
        System.err.println("[stopRow]            scan stop at the end key");
        System.err
                .println("[SQL]                select row_key,{field1},{field2}");
        System.err.println("                     from {table}");
        System.err
                .println("                     where {field1} = {value1} and {field2} != {value2}");
        System.err
                .println("[filterIfMissing]    filter when column not exists");
        System.err
                .println("[output]             output file path, default is stdout");
        System.exit(-1);
    }

    private void parseArgs(String[] args) throws IOException {
        if (args.length < 5) {
            usage();
        }
        this.familyName = args[0].getBytes();
        this.startRow = args[1];
        this.stopRow = args[2];
        String strSql = args[3];

        this.sql = HBaseSQLParser.parse(strSql);
        this.tableName = this.sql.getTable();

        if (args.length >= 5) {
            this.filterIfMissing = Boolean.parseBoolean(args[4]);
        }

        if (args.length >= 6) {
            writer = new BufferedWriter(new FileWriter(args[5], false), 40960);
        }
    }

    public static void main(String[] args) {
        try {
            long start = System.currentTimeMillis();
            HBaseQuery instance = new HBaseQuery();
            instance.parseArgs(args);
            instance.query();
            instance.close();
            long consumes = System.currentTimeMillis() - start;
            System.out.println("Rows: " + instance.counter);
            System.out.println("Use Time(s): " + consumes / 1000f);
        } catch (IOException e) {
            System.err.println("Create table failed.");
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Create table failed.");
            e.printStackTrace();
        }
    }
}
