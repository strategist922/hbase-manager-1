package com.wandoujia.hbase.manager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author fengzanfeng
 */
public class HBaseMultiThreadClient {
    public static final String defaultFamily = "c";

    public static final int defaultThreadNum = 10;

    private HTablePool tablePool;

    public HBaseMultiThreadClient() {
        this(defaultThreadNum);
    }

    public HBaseMultiThreadClient(int threadNum) {
        tablePool = new HTablePool(HBaseConfiguration.create(), threadNum);
    }

    /**
     * Initial HBase client with set write buffer size
     * 
     * @param poolSize
     * @param writeBufferSize
     */
    public HBaseMultiThreadClient(int threadNum, final long writeBufferSize) {
        tablePool = new HTablePool(HBaseConfiguration.create(), threadNum,
                new HTableFactory() {
                    @Override
                    public HTableInterface createHTableInterface(
                            Configuration config, byte[] tableName) {
                        try {
                            HTable hTable = new HTable(config, tableName);
                            hTable.setAutoFlush(false);
                            hTable.setWriteBufferSize(writeBufferSize);
                            return hTable;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    /**
     * @param tableName
     * @throws IOException
     */
    public void flush(String tableName) throws IOException {
        HTable hTable = (HTable) tablePool.getTable(tableName);
        try {
            hTable.flushCommits();
        } finally {
            if (hTable != null) {
                tablePool.putTable((HTableInterface) hTable);
            }
        }
    }

    /**
     * @throws IOException
     */
    public void close() throws IOException {
        tablePool.close();
    }

    /**
     * @param tableName
     * @throws IOException
     */
    public void close(String tableName) throws IOException {
        tablePool.closeTablePool(Bytes.toBytes(tableName));
    }

    /**
     * @param tableName
     * @param rowKey
     * @param family
     * @param columns
     * @return
     * @throws IOException
     */
    public Map<String, byte[]> get(String tableName, String rowKey,
            String family, Set<String> columns, boolean cacheBlocks)
            throws IOException {
        HTable hTable = (HTable) tablePool.getTable(tableName);
        try {
            Result result = null;
            Get get = new Get(rowKey.getBytes());
            get.setCacheBlocks(cacheBlocks);
            for (String column: columns) {
                get.addColumn(family.getBytes(), column.getBytes());
            }
            result = hTable.get(get);
            Map<String, byte[]> ret = new HashMap<String, byte[]>();
            for (KeyValue kv: result.raw()) {
                ret.put(new String(kv.getQualifier()), kv.getValue());
            }
            return ret;
        } finally {
            if (hTable != null) {
                tablePool.putTable((HTableInterface) hTable);
            }
        }
    }

    /**
     * @param tableName
     * @param rowKey
     * @param family
     * @param values
     * @throws IOException
     */
    public void update(String tableName, String rowKey, String family,
            Map<String, byte[]> values) throws IOException {
        HTable hTable = (HTable) tablePool.getTable(tableName);

        try {
            Put put = new Put(rowKey.getBytes());
            for (Map.Entry<String, byte[]> entry: values.entrySet()) {
                put.add(family.getBytes(), entry.getKey().getBytes(),
                        entry.getValue());
            }
            hTable.put(put);
        } finally {
            if (hTable != null) {
                tablePool.putTable((HTableInterface) hTable);
            }
        }
    }

    /**
     * @param tableName
     * @param startRow
     * @param cacheBlocks
     * @return
     * @throws IOException
     */
    public ResultScanner getScanner(String tableName, String startRow,
            boolean cacheBlocks) throws IOException {
        Scan scan = new Scan(Bytes.toBytes(startRow));
        scan.setCacheBlocks(cacheBlocks);
        HTable hTable = (HTable) tablePool.getTable(tableName);
        try {
            ResultScanner scanner = hTable.getScanner(scan);
            return scanner;
        } finally {
            if (hTable != null) {
                tablePool.putTable((HTableInterface) hTable);
            }
        }
    }

    /**
     * @param tableName
     * @param startRow
     * @param stopRow
     * @param filters
     * @param opers
     * @param cacheBlocks
     * @return
     * @throws IOException
     */
    public ResultScanner getScanner(String tableName, String startRow,
            String stopRow, Map<String, String> filters,
            Map<String, CompareOp> opers, boolean cacheBlocks)
            throws IOException {
        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
        scan.setCacheBlocks(cacheBlocks);
        if (filters != null && opers != null) {
            FilterList filterList = new FilterList();
            for (Map.Entry<String, String> entry: filters.entrySet()) {
                filterList.addFilter(new SingleColumnValueFilter(Bytes
                        .toBytes(defaultFamily), Bytes.toBytes(entry.getKey()),
                        opers.get(entry.getKey()), Bytes.toBytes(entry
                                .getValue())));
            }
            scan.setFilter(filterList);
        }
        HTable hTable = (HTable) tablePool.getTable(tableName);
        try {
            ResultScanner scanner = hTable.getScanner(scan);
            return scanner;
        } finally {
            if (hTable != null) {
                tablePool.putTable((HTableInterface) hTable);
            }
        }
    }

    /**
     * @param tableName
     * @param rowKey
     * @param family
     * @param values
     * @throws IOException
     */
    public void insert(String tableName, String rowKey, String family,
            Map<String, byte[]> values) throws IOException {
        update(tableName, rowKey, family, values);
    }

    /**
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public void delete(String tableName, String rowKey) throws IOException {
        HTable hTable = (HTable) tablePool.getTable(tableName);
        try {
            Delete delete = new Delete(rowKey.getBytes());
            hTable.delete(delete);
        } finally {
            if (hTable != null) {
                tablePool.putTable((HTableInterface) hTable);
            }
        }
    }
}
