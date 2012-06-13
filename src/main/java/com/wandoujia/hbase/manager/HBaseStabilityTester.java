package com.wandoujia.hbase.manager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;

import com.wandoujia.stresstest.Config;
import com.wandoujia.stresstest.ITester;
import com.wandoujia.stresstest.StressTestSuit;

public class HBaseStabilityTester implements ITester<Integer, Integer> {

    private String tableName = null;

    List<String> paths = new ArrayList<String>();

    private HTablePool hTablePool;

    private Random random = new Random();

    public static String FAMILY = "c";

    public static String QUALIFIER = "d";

    public HBaseStabilityTester(String path, String tableName)
            throws IOException {

        this.tableName = tableName;

        BufferedReader reader = new BufferedReader(new FileReader(path));
        String line = null;
        while ((line = reader.readLine()) != null) {
            paths.add(line.split(" ")[0]);
        }
        System.out.println("paths num: " + paths.size());

        hTablePool = new HTablePool(HBaseConfiguration.create(), 100);
    }

    private synchronized HTable getHtable(String tableName) {
        return (HTable) hTablePool.getTable(tableName);
    }

    public static void usage() {
        System.err
                .println("HBaseStabilityTester <paths_file> <hbase_table> <thread_num> <request_per_thread>");
        System.exit(1);
    }

    private static Config getConfig(int threadNum, int requestPerThread) {
        Config config = new Config();
        config.setReadThreadCount(threadNum);
        config.setReadCountPerThread(requestPerThread);
        return config;
    }

    public static void main(String[] args) throws IOException {

        if (args.length != 4) {
            usage();
        }

        String path = args[0];
        String tableName = args[1];
        int threadNum = Integer.parseInt(args[2]);
        int requestPerThread = Integer.parseInt(args[3]);

        StressTestSuit<Integer, Integer> testsuit = new StressTestSuit<Integer, Integer>();
        testsuit.setTester(new HBaseStabilityTester(path, tableName));
        testsuit.setConfig(getConfig(threadNum, requestPerThread));
        testsuit.init();
        testsuit.start();

    }

    public void before() throws Exception {}

    public void after() throws Exception {}

    public Integer beforeRead() throws Exception {
        return null;
    }

    public void read(Integer val) throws Exception {
        long start = System.currentTimeMillis();

        HTable hTable = getHtable(tableName);
        int randomLineNum = this.random.nextInt(paths.size());
        String path = paths.get(randomLineNum);

        Result result = null;
        Get get = new Get(path.getBytes());
        get.addColumn(FAMILY.getBytes(), QUALIFIER.getBytes());
        result = hTable.get(get);
        for (KeyValue kv: result.raw()) {
            byte[] data = kv.getValue();
            System.out.println("time: " + (System.currentTimeMillis() - start)
                    + " size: " + data.length);
            break;
        }
    }

    public Integer beforeWrite() throws Exception {
        return null;
    }

    public void write(Integer val) throws Exception {}
}
