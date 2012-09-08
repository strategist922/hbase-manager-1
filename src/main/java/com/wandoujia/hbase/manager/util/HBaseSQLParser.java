package com.wandoujia.hbase.manager.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.filter.FilterList.Operator;

import com.wandoujia.hbase.manager.util.Condition.SQLCompareOp;

public class HBaseSQLParser {
    /**
     * SELECT {field},{field} FROM {table} WHERE {field} = {value} and {field}
     * != {value} and {field} <= {value}
     */

    public static String SELECT = "SELECT ";

    public static String FROM = " FROM ";

    public static String WHERE = " WHERE ";

    private static int getIndex(String str, String subStr) {
        int index = str.indexOf(subStr.toUpperCase());
        if (index == -1) {
            return str.indexOf(subStr.toLowerCase());
        }
        return index;
    }

    private static String getTable(String strSql, int selectIndex,
            int fromIndex, int whereIndex) {
        if (whereIndex != -1) {
            return strSql.substring(fromIndex + 6, whereIndex);
        } else {
            return strSql.substring(fromIndex + 6, strSql.length());
        }
    }

    private static List<String> getFields(String strSql, int selectIndex,
            int fromIndex, int whereIndex) {
        String strFields = strSql.substring(selectIndex + 7, fromIndex).trim()
                .replace(" ", "");
        if (StringUtils.isEmpty(strFields)) {
            return null;
        }
        if (strFields.trim().equals("*")) {
            return null;
        }
        String[] fields = strFields.split(",");
        return Arrays.asList(fields);
    }

    private static List<Condition> getConditions(String strSql,
            int selectIndex, int fromIndex, int whereIndex) throws IOException {
        if (whereIndex == -1) {
            return null;
        }
        List<Condition> conditions = new ArrayList<Condition>();
        String strConditions = strSql.substring(whereIndex + 6).trim();
        if (StringUtils.isEmpty(strConditions)) {
            throw new IOException("syntax error near in where");
        }
        String[] arr = null;
        if (strConditions.indexOf(" and ") != -1) {
            arr = strConditions.split(" and ");
        } else if (strConditions.indexOf(" or ") != -1) {
            arr = strConditions.split(" or ");
        } else {
            arr = new String[1];
            arr[0] = strConditions;
        }
        for (String strCondition: arr) {
            String[] tmp = strCondition.split(" ");
            if (tmp.length != 3) {
                throw new IOException("syntax error near in where");
            }
            Condition c = new Condition();
            c.setField(tmp[0].trim());
            c.setSqlCompareOp(SQLCompareOp.parse(tmp[1].trim()));
            c.setValue(tmp[2].trim());
            if (c.getSqlCompareOp() == null) {
                throw new IOException("syntax error");
            }
            conditions.add(c);
        }
        return conditions;
    }

    private static Operator getOperator(String strSql) {
        if (strSql.indexOf(" and ") != -1) {
            return Operator.MUST_PASS_ALL;
        } else {
            return Operator.MUST_PASS_ONE;
        }
    }

    public static SQL parse(String strSql) throws IOException {
        SQL sql = new SQL();
        if (StringUtils.isEmpty(strSql)) {
            return null;
        }
        strSql = strSql.replace("'", "").trim();

        if (strSql.indexOf(" and ") == -1 && strSql.indexOf(" or ") != -1) {
            throw new IOException("syntax error, exclusive or/and");
        }

        int selectIndex = getIndex(strSql, SELECT);
        int fromIndex = getIndex(strSql, FROM);
        int whereIndex = getIndex(strSql, WHERE);

        if (selectIndex == -1 || fromIndex == -1) {
            throw new IOException("syntax error");
        }

        sql.setTable(getTable(strSql, selectIndex, fromIndex, whereIndex));
        sql.setOperator(getOperator(strSql));
        List<String> fields = getFields(strSql, selectIndex, fromIndex,
                whereIndex);
        List<Condition> conditions = getConditions(strSql, selectIndex,
                fromIndex, whereIndex);

        sql.setFields(fields);
        sql.setConditions(conditions);
        return sql;
    }

    public static void main(String[] args) throws IOException {
        SQL sql = HBaseSQLParser
                .parse("SELECT field1,field2 FROM table WHERE field = value and {field} != {value} and {field} like {value} and {field} <= {value}");
        System.out.println(sql.toString());
    }
}
