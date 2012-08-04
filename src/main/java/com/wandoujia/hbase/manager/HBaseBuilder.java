package com.wandoujia.hbase.manager;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseBuilder {
    private static final Logger LOG = LoggerFactory
            .getLogger(HBaseBuilder.class);

    public static String defaultFamily = "content";

    private static int scannerCacheing = 10000;

    private Configuration conf;

    private HBaseAdmin admin;

    /**
     * Default table to use for update/scan operations.
     */
    private HTable table;

    protected HBaseBuilder() {
        conf = HBaseConfiguration.create();
        LOG.info("zookeeper quorum: {}", conf.get(HConstants.ZOOKEEPER_QUORUM));
    }

    protected HBaseBuilder(Configuration conf) {
        this.conf = conf;
        LOG.info("zookeeper quorum: {}", conf.get(HConstants.ZOOKEEPER_QUORUM));
    }

    public static HBaseBuilder connect() {
        return new HBaseBuilder();
    }

    /**
     * @param host
     *            zookeeper host
     * @return
     */
    public static HBaseBuilder connect(String host) {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, host);
        return new HBaseBuilder(conf);
    }

    /**
     * @param conf
     * @return
     */
    public static HBaseBuilder connect(HBaseConfiguration conf) {
        return new HBaseBuilder(conf);
    }

    public static HBaseBuilder connect(Map<String, Object> args)
            throws IOException {
        String host = (String) args.remove("host");
        String tableName = (String) args.remove("table");
        String confURL = (String) args.remove("confURL");
        Configuration conf = HBaseConfiguration.create();
        if (confURL != null) {
            conf.addResource(new URL(confURL));
        }
        for (Map.Entry<String, Object> entry: args.entrySet()) {
            String key = entry.getKey();
            Object val = entry.getValue();
            if (val == null) {
                continue;
            } else if (val instanceof Integer) {
                conf.setInt(key, (Integer) val);
            } else if (val instanceof Long) {
                conf.setLong(key, (Long) val);
            } else if (val instanceof Boolean) {
                conf.setBoolean(key, (Boolean) val);
            } else if (val instanceof String[]) {
                conf.setStrings(key, (String[]) val);
            } else {
                conf.set(key, val.toString());
            }
        }
        if (host != null) {
            conf.set(HConstants.ZOOKEEPER_QUORUM, host);
        }
        HBaseBuilder hb = new HBaseBuilder(conf);
        hb.setTableName(tableName);
        return hb;
    }

    public static byte[][] getHexSplits(String startKey, String endKey,
            int numRegions) {
        byte[][] splits = new byte[numRegions - 1][];
        BigInteger lowestKey = new BigInteger(startKey, 16);
        BigInteger highestKey = new BigInteger(endKey, 16);
        BigInteger range = highestKey.subtract(lowestKey);
        BigInteger regionIncrement = range.divide(BigInteger
                .valueOf(numRegions));
        lowestKey = lowestKey.add(regionIncrement);
        for (int i = 0; i < numRegions - 1; i++) {
            BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger
                    .valueOf(i)));
            byte[] b = String.format("%016x", key).getBytes();
            splits[i] = b;
        }
        return splits;
    }

    public HTableDescriptor getTableDescriptor(String tableName,
            String familyName, BloomType bloomType, Algorithm compressionType,
            Boolean inMemory, Boolean blockCacheEnabled, int blockSize,
            int maxVersions) {
        HTableDescriptor table = new HTableDescriptor(tableName);
        HColumnDescriptor hcd = new HColumnDescriptor(familyName);
        hcd.setBloomFilterType(bloomType);
        hcd.setCompressionType(compressionType);
        hcd.setInMemory(inMemory);
        hcd.setBlockCacheEnabled(blockCacheEnabled);
        hcd.setBlocksize(blockSize);
        hcd.setMaxVersions(maxVersions);

        // other descriptor
        hcd.setScope(0);
        hcd.setTimeToLive(-1);
        table.addFamily(hcd);

        return table;
    }

    public HTableDescriptor create(String tableName, String familyName,
            BloomType bloomType, Algorithm compressionType, Boolean inMemory,
            Boolean blockCacheEnabled, int blockSize, int maxVersions,
            byte[][] splits) throws IOException {
        if (getAdmin().tableExists(tableName)) {
            LOG.warn("{} table has exists.", tableName);
            return null;
        }
        HTableDescriptor table = getTableDescriptor(tableName, familyName,
                bloomType, compressionType, inMemory, blockCacheEnabled,
                blockSize, maxVersions);
        getAdmin().createTable(table, splits);
        return table;
    }

    public void drop(String tableName) throws IOException {
        getAdmin().disableTable(tableName);
        getAdmin().deleteTable(tableName);
    }

    public void enable(String tableName) throws IOException {
        getAdmin().enableTable(tableName);
    }

    public void disable(String tableName) throws IOException {
        getAdmin().disableTable(tableName);
    }

    public HTableDescriptor[] listTables() throws IOException {
        return getAdmin().listTables();
    }

    public HTableDescriptor showSchema(String tableName) throws IOException {
        return getAdmin().getTableDescriptor(Bytes.toBytes(tableName));
    }

    public HBaseAdmin getAdmin() throws IOException {
        if (this.admin == null)
            this.admin = new HBaseAdmin(this.conf);
        return this.admin;
    }

    public void setTableName(String name) throws IOException {
        if (name == null || name.length() < 1) {
            this.table = null;
            return;
        }
        setTable(new HTable(conf, name));
    }

    public void setTable(HTable table) {
        this.table = table;
        this.table.setScannerCaching(scannerCacheing);
    }

    public HTable getTable() {
        return this.table;
    }

    /**
     * @param rowKey
     * @param family
     * @param qualifiers
     * @return
     * @throws IOException
     */
    public synchronized Map<String, byte[]> getRow(String rowKey,
            String family, Set<String> qualifiers) throws IOException {
        HTable table = getTable();
        Result result = null;
        Get get = new Get(rowKey.getBytes());
        for (String qualifier: qualifiers) {
            get.addColumn(family.getBytes(), qualifier.getBytes());
        }
        result = table.get(get);
        Map<String, byte[]> ret = new HashMap<String, byte[]>();
        for (KeyValue kv: result.raw()) {
            ret.put(new String(kv.getQualifier()), kv.getValue());
        }
        return ret;
    }

    public synchronized long countByFilter(String startRow, String stopRow,
            Map<String, String> filters, Map<String, CompareOp> opers)
            throws IOException {
        long result = 0;

        ResultScanner scanner = this.select(startRow, stopRow, filters, opers);

        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
            Map<String, String> record = getRecord(rr);
            if (!isValid(record, filters)) {
                continue;
            }
            result++;
        }
        scanner.close();
        return result;
    }

    public synchronized long deleteByFilter(String startRow, String stopRow,
            Map<String, String> filters, Map<String, CompareOp> opers)
            throws IOException {
        long affectRows = 0;
        ResultScanner scanner = this.select(startRow, stopRow, filters, opers);

        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
            Map<String, String> record = getRecord(rr);
            if (!isValid(record, filters)) {
                continue;
            }
            Delete delete = new Delete(rr.getRow());
            table.delete(delete);
            affectRows++;
        }
        table.flushCommits();
        scanner.close();
        return affectRows;
    }

    public Map<String, String> getRecord(Result result) {
        Map<String, String> record = new HashMap<String, String>();
        for (KeyValue kv: result.list()) {
            if (!record.containsKey("a_row")) {
                record.put("a_row", new String(kv.getRow()));
            }
            String key = new String(kv.getQualifier());
            String value = new String(kv.getValue());
            record.put(key, value);
        }
        return record;
    }

    public boolean isValid(Map<String, String> record,
            Map<String, String> filters) {
        for (Map.Entry<String, String> entry: filters.entrySet()) {
            if (!record.containsKey(entry.getKey())) {
                return false;
            }
        }
        return true;
    }

    public synchronized List<Map<String, String>> selectByFilter(
            String startRow, String stopRow, Map<String, String> filters,
            Map<String, CompareOp> opers) throws IOException {

        int readCounter = 0;
        int maxReadCounter = 300000;

        List<Map<String, String>> results = new ArrayList<Map<String, String>>();
        ResultScanner scanner = this.select(startRow, stopRow, filters, opers);
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
            Map<String, String> record = getRecord(rr);
            if (!isValid(record, filters)) {
                continue;
            }
            readCounter++;
            results.add(record);
            if (readCounter >= maxReadCounter) {
                LOG.warn("scaner breaked!!! has more record...");
                LOG.warn("scaner breaked!!! has more record...");
                LOG.warn("scaner breaked!!! has more record...");
                break;
            }
        }
        scanner.close();
        return results;
    }

    public synchronized long dumpByFilter(String startRow, String stopRow,
            Map<String, String> filters, Map<String, CompareOp> opers,
            String[] qualifiers, BufferedWriter writer) throws IOException {

        long rows = 0;
        HashMap<String, String> map = new HashMap<String, String>();
        for (String qualifier: qualifiers) {
            map.put(qualifier, "");
        }
        ResultScanner scanner = this.select(startRow, stopRow, filters, opers);
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
            Map<String, String> record = getRecord(rr);
            if (!isValid(record, filters)) {
                continue;
            }

            map.put("row_key", new String(rr.getRow()));
            for (Map.Entry<String, String> entry: record.entrySet()) {
                if (map.containsKey(entry.getKey())) {
                    map.put(entry.getKey(), entry.getValue());
                }
            }

            rows += 1;
            int len = qualifiers.length;
            String line = "";
            for (int i = 0; i < len; i++) {
                String qualifier = qualifiers[i];
                if (i == (len - 1)) {
                    line += map.get(qualifier);
                } else {
                    line += map.get(qualifier) + "\t";
                }
                map.put(qualifier, "");
            }
            writer.write(line + "\n");
        }
        scanner.close();
        writer.close();
        return rows;
    }

    public synchronized ResultScanner select(String startRow, String stopRow,
            Map<String, String> filters, Map<String, CompareOp> opers)
            throws IOException {

        FilterList filterList = new FilterList();
        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));

        for (Map.Entry<String, String> entry: filters.entrySet()) {
            filterList
                    .addFilter(new SingleColumnValueFilter(Bytes
                            .toBytes(defaultFamily), Bytes.toBytes(entry
                            .getKey()), opers.get(entry.getKey()), Bytes
                            .toBytes(entry.getValue())));
        }

        scan.setFilter(filterList);
        ResultScanner ResultScannerFilterList = table.getScanner(scan);
        return ResultScannerFilterList;
    }

    public void update(String rowKey, String family, Map<String, byte[]> values)
            throws IOException {
        HTable table = getTable();
        Put put = new Put(rowKey.getBytes());
        for (Map.Entry<String, byte[]> entry: values.entrySet()) {
            put.add(family.getBytes(), entry.getKey().getBytes(),
                    entry.getValue());
        }
        table.put(put);
    }

    public void insert(String rowKey, String family, Map<String, byte[]> values)
            throws IOException {
        update(rowKey, family, values);
    }

    public void delete(String rowKey) throws IOException {
        HTable table = getTable();
        Delete delete = new Delete(rowKey.getBytes());
        table.delete(delete);
    }
}
