package com.wandoujia.hbase.manager.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * this client is not thread safe for reads and writes.
 * 
 * @author fengzanfeng
 */
public class HBaseClient {

    public static Configuration conf = HBaseConfiguration.create();

    private HBaseAdmin admin;

    private byte[] familyName;

    private HTable table;

    public HBaseClient() {
        this.familyName = HBaseUtil.defaultFamilyName;
    }

    public HBaseClient(String tableName) throws IOException {
        this.familyName = HBaseUtil.defaultFamilyName;
        this.table = new HTable(conf, tableName);
    }

    public HBaseClient(String tableName, byte[] familyName) throws IOException {
        this.familyName = familyName;
        this.table = new HTable(conf, tableName);
    }

    /**
     * set scanner caching row numbers.
     * 
     * @param scannerCaching
     */
    public void setScannerCaching(int scannerCaching) {
        this.table.setScannerCaching(scannerCaching);
    }

    /**
     * set write buffer size
     * 
     * @param writeBufferSize
     * @throws IOException
     */
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
        this.table.setAutoFlush(false);
        this.table.setWriteBufferSize(writeBufferSize);
    }

    /**
     * @return
     * @throws IOException
     */
    public HBaseAdmin getAdmin() throws IOException {
        if (this.admin == null)
            this.admin = new HBaseAdmin(conf);
        return this.admin;
    }

    /**
     * @return
     */
    public HTable getTable() {
        return this.table;
    }

    /**
     * @return
     */
    public byte[] getFamilyName() {
        return this.familyName;
    }

    /**
     * create hbase table
     * 
     * @param tableName
     * @param familyName
     * @param bloomType
     * @param compressionType
     * @param inMemory
     * @param blockCacheEnabled
     * @param blockSize
     * @param maxVersions
     * @param splits
     * @return
     * @throws IOException
     */
    public HTableDescriptor createHTable(String tableName, byte[] familyName,
            BloomType bloomType, Algorithm compressionType, Boolean inMemory,
            Boolean blockCacheEnabled, int blockSize, int maxVersions,
            byte[][] splits) throws IOException {
        if (getAdmin().tableExists(tableName)) {
            return null;
        }
        HTableDescriptor table = HBaseUtil.generateTableDescriptor(tableName,
                familyName, bloomType, compressionType, inMemory,
                blockCacheEnabled, blockSize, maxVersions);
        getAdmin().createTable(table, splits);
        return table;
    }

    /**
     * @param tableName
     * @throws IOException
     */
    public void drop(String tableName) throws IOException {
        getAdmin().disableTable(tableName);
        getAdmin().deleteTable(tableName);
    }

    /**
     * @param tableName
     * @throws IOException
     */
    public void enable(String tableName) throws IOException {
        getAdmin().enableTable(tableName);
    }

    /**
     * @param tableName
     * @throws IOException
     */
    public void disable(String tableName) throws IOException {
        getAdmin().disableTable(tableName);
    }

    /**
     * list all tables of cluster
     * 
     * @return
     * @throws IOException
     */
    public HTableDescriptor[] listTables() throws IOException {
        return getAdmin().listTables();
    }

    /**
     * @param tableName
     * @return
     * @throws IOException
     */
    public HTableDescriptor showSchema(String tableName) throws IOException {
        return getAdmin().getTableDescriptor(Bytes.toBytes(tableName));
    }

    /**
     * get a single row with a single column
     * 
     * @param rowKey
     * @param column
     * @param cacheBlocks
     * @return
     * @throws IOException
     */
    public synchronized byte[] getRow(String rowKey, byte[] column,
            boolean cacheBlocks) throws IOException {
        HTable table = getTable();
        Get get = new Get(rowKey.getBytes());
        get.setCacheBlocks(cacheBlocks);
        get.addColumn(familyName, column);
        Result row = table.get(get);
        if (row.raw().length >= 1) {
            return row.raw()[0].getValue();
        }
        return null;
    }

    /**
     * get a single with multi-columns
     * 
     * @param rowKey
     * @param columns
     * @param cacheBlocks
     * @return
     * @throws IOException
     */
    public synchronized Map<String, byte[]> getRow(String rowKey,
            Set<byte[]> columns, boolean cacheBlocks) throws IOException {
        HTable table = getTable();
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
    }

    /**
     * get scanner
     * 
     * @param startRow
     * @param stopRow
     * @param cacheBlocks
     * @return
     * @throws IOException
     */
    public synchronized ResultScanner getScanner(String startRow,
            String stopRow, int caching, boolean cacheBlocks)
            throws IOException {
        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
        scan.setCacheBlocks(cacheBlocks);
        scan.setCaching(caching);
        return table.getScanner(scan);
    }

    /**
     * get scanner with filter
     * 
     * @param startRow
     * @param stopRow
     * @param filter
     * @param cacheBlocks
     * @return
     * @throws IOException
     */
    public synchronized ResultScanner getScanner(String startRow,
            String stopRow, Filter filter, int caching, boolean cacheBlocks)
            throws IOException {
        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
        scan.setCacheBlocks(cacheBlocks);
        scan.setCaching(caching);
        if (filter != null) {
            scan.setFilter(filter);
        }
        return table.getScanner(scan);
    }

    /**
     * update a row
     * 
     * @param rowKey
     * @param values
     * @throws IOException
     */
    public void update(String rowKey, Map<String, byte[]> values)
            throws IOException {
        HTable table = getTable();
        Put put = new Put(rowKey.getBytes());
        for (Map.Entry<String, byte[]> entry: values.entrySet()) {
            put.add(this.familyName, entry.getKey().getBytes(),
                    entry.getValue());
        }
        table.put(put);
    }

    /**
     * insert a row
     * 
     * @param rowKey
     * @param values
     * @throws IOException
     */
    public void insert(String rowKey, Map<String, byte[]> values)
            throws IOException {
        update(rowKey, values);
    }

    /**
     * delete a row
     * 
     * @param rowKey
     * @throws IOException
     */
    public void delete(String rowKey) throws IOException {
        HTable table = getTable();
        Delete delete = new Delete(rowKey.getBytes());
        table.delete(delete);
    }

}
