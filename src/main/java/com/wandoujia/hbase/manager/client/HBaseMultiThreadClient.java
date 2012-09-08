package com.wandoujia.hbase.manager.client;

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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * this client is thread safe for reads and writes.
 * 
 * @author fengzanfeng
 */
public class HBaseMultiThreadClient {

    private HTablePool tablePool;

    /**
     * Inital hbase client with default hbase table pool size.
     */
    public HBaseMultiThreadClient() {
        this(HBaseUtil.defaultPoolSize);
    }

    /**
     * Inital hbase client, set hbase table pool size
     * 
     * @param poolSize
     */
    public HBaseMultiThreadClient(int poolSize) {
        tablePool = new HTablePool(HBaseConfiguration.create(), poolSize);
    }

    /**
     * Initial hbase client, set hbase table pool size and write buffer size.
     * please call close() for safed exit, otherwise will cause data lose.
     * 
     * @param poolSize
     * @param writeBufferSize
     */
    public HBaseMultiThreadClient(int poolSize, final long writeBufferSize) {
        tablePool = new HTablePool(HBaseConfiguration.create(), poolSize,
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
        HTableInterface hTable = tablePool.getTable(tableName);
        try {
            hTable.flushCommits();
        } finally {
            if (hTable != null) {
                tablePool.putTable(hTable);
            }
        }
    }

    /**
     * close hbase table pool
     * 
     * @throws IOException
     */
    public void close() throws IOException {
        tablePool.close();
    }

    /**
     * close hbase table
     * 
     * @param tableName
     * @throws IOException
     */
    public void close(String tableName) throws IOException {
        tablePool.closeTablePool(tableName.getBytes());
    }

    /**
     * get a single row with a single column
     * 
     * @param tableName
     * @param familyName
     * @param rowKey
     * @param column
     * @param cacheBlocks
     * @return
     * @throws IOException
     */
    public byte[] getRow(String tableName, byte[] familyName, String rowKey,
            byte[] column, boolean cacheBlocks) throws IOException {
        HTableInterface table = tablePool.getTable(tableName);
        try {
            Get get = new Get(rowKey.getBytes());
            get.setCacheBlocks(cacheBlocks);
            get.addColumn(familyName, column);
            Result row = table.get(get);
            if (row.raw().length >= 1) {
                return row.raw()[0].getValue();
            }
            return null;
        } finally {
            if (table != null) {
                tablePool.putTable(table);
            }
        }
    }

    /**
     * get a single with multi-columns
     * 
     * @param tableName
     * @param familyName
     * @param rowKey
     * @param columns
     * @param cacheBlocks
     * @return
     * @throws IOException
     */
    public Map<String, byte[]> getRow(String tableName, byte[] familyName,
            String rowKey, Set<byte[]> columns, boolean cacheBlocks)
            throws IOException {
        HTableInterface table = tablePool.getTable(tableName);
        try {
            Get get = new Get(rowKey.getBytes());
            get.setCacheBlocks(cacheBlocks);
            for (byte[] column: columns) {
                get.addColumn(familyName, column);
            }
            Result row = table.get(get);
            if (row.raw().length >= 1) {
                Map<String, byte[]> result = new HashMap<String, byte[]>();
                for (KeyValue kv: row.raw()) {
                    result.put(new String(kv.getQualifier()), kv.getValue());
                }
                return result;
            }
            return null;
        } finally {
            if (table != null) {
                tablePool.putTable(table);
            }
        }
    }

    /**
     * @param tableName
     * @param familyName
     * @param startRow
     * @param stopRow
     * @param caching
     * @param cacheBlocks
     * @return
     * @throws IOException
     */
    public ResultScanner getScanner(String tableName, byte[] familyName,
            String startRow, String stopRow, int caching, boolean cacheBlocks)
            throws IOException {
        HTableInterface table = tablePool.getTable(tableName);
        try {
            Scan scan = new Scan(Bytes.toBytes(startRow),
                    Bytes.toBytes(stopRow));
            scan.setCacheBlocks(cacheBlocks);
            scan.setCaching(caching);
            return table.getScanner(scan);
        } finally {
            if (table != null) {
                tablePool.putTable(table);
            }
        }
    }

    /**
     * @param tableName
     * @param familyName
     * @param startRow
     * @param stopRow
     * @param filter
     *            See @HBaseUtil
     * @param caching
     * @param cacheBlocks
     * @return
     * @throws IOException
     */
    public ResultScanner getScanner(String tableName, byte[] familyName,
            String startRow, String stopRow, Filter filter, int caching,
            boolean cacheBlocks) throws IOException {
        HTableInterface table = tablePool.getTable(tableName);
        try {
            Scan scan = new Scan(Bytes.toBytes(startRow),
                    Bytes.toBytes(stopRow));
            scan.setCacheBlocks(cacheBlocks);
            scan.setCaching(caching);
            if (filter != null) {
                scan.setFilter(filter);
            }
            return table.getScanner(scan);
        } finally {
            if (table != null) {
                tablePool.putTable(table);
            }
        }
    }

    /**
     * @param tableName
     * @param familyName
     * @param rowKey
     * @param values
     * @throws IOException
     */
    public void update(String tableName, byte[] familyName, String rowKey,
            Map<String, byte[]> values) throws IOException {
        HTableInterface table = tablePool.getTable(tableName);
        try {
            Put put = new Put(rowKey.getBytes());
            for (Map.Entry<String, byte[]> entry: values.entrySet()) {
                put.add(familyName, entry.getKey().getBytes(), entry.getValue());
            }
            table.put(put);
        } finally {
            if (table != null) {
                tablePool.putTable(table);
            }
        }
    }

    /**
     * @param tableName
     * @param familyName
     * @param rowKey
     * @param values
     * @throws IOException
     */
    public void insert(String tableName, byte[] familyName, String rowKey,
            Map<String, byte[]> values) throws IOException {
        update(tableName, familyName, rowKey, values);
    }

    /**
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public void delete(String tableName, String rowKey) throws IOException {
        HTableInterface table = tablePool.getTable(tableName);
        try {
            Delete delete = new Delete(rowKey.getBytes());
            table.delete(delete);
        } finally {
            if (table != null) {
                tablePool.putTable(table);
            }
        }
    }
}
