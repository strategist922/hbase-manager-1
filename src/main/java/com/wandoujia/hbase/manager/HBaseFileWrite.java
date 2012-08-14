package com.wandoujia.hbase.manager;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.wandoujia.misc.utils.HashUtils;

public class HBaseFileWrite {

    public static void usage() {
        System.err.println("HBaseFileWrite <path> <table> <output:keys_file>");
        System.err.println("path: ");
        System.exit(1);
    }

    public static void listFiles(String path, List<String> result) {
        File file = new File(path);
        if (file.isFile()) {
            result.add(file.getAbsolutePath());
        } else {
            File[] files = file.listFiles();
            for (File tmpFile: files) {
                if (tmpFile.isFile()) {
                    result.add(tmpFile.getAbsolutePath());
                } else {
                    listFiles(tmpFile.getAbsolutePath(), result);
                }
            }
        }
    }

    public static byte[] readFile(String path) throws IOException {
        File file = new File(path);
        long fileLen = file.length();
        FileInputStream in = new FileInputStream(path);
        byte[] buff = new byte[(int) fileLen];
        in.read(buff, 0, (int) fileLen);
        return buff;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            usage();
        }
        String readPath = args[0];
        String tableName = args[1];
        String keysPath = args[2];
        FileWriter writer = new FileWriter(new File(keysPath));

        List<String> fileList = new ArrayList<String>();
        listFiles(readPath, fileList);
        System.out.println("files num: " + fileList.size());

        HBaseClient hb = new HBaseClient();
        hb.setTableName(tableName);

        for (String path: fileList) {
            System.out.println("read file: " + path);
            byte[] fileData = readFile(path);
            String rowKey = HashUtils.md5DigestStr(fileData);
            Map<String, byte[]> values = new HashMap<String, byte[]>();
            values.put("d", fileData);
            hb.insert(rowKey, "c", values);
            writer.write(rowKey + " " + path + "\n");
        }
        writer.close();

    }
}
