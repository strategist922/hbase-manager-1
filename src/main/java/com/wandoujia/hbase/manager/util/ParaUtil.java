package com.wandoujia.hbase.manager.util;

import java.util.Map;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

public class ParaUtil {
    public static void parseHBaseExpressions(String expressions,
            Map<String, String> filters, Map<String, CompareOp> opers) {
        expressions = expressions.replace(" ", "");
        if (expressions.equals("-")) {
            return;
        }
        for (String expression: expressions.split(",")) {
            String[] arr = expression.split(":");
            if (arr.length != 3) {
                System.out.println(arr.length);
                throw new RuntimeException("illegal expression: " + expression);
            }
            String key = arr[0];
            String oper = arr[1];
            String value = arr[2];

            if (oper.equals("==")) {
                // The value stored in the row or column must be equal to the
                // specified value.
                filters.put(key, value);
                opers.put(key, CompareOp.EQUAL);
            } else if (oper.equals("!=")) {
                // The value stored in the row or column must not be equal to
                // the specified value.
                filters.put(key, value);
                opers.put(key, CompareOp.NOT_EQUAL);
            } else if (oper.equals("greater")) {
                // The value stored in the row or column must be greater than
                // the specified value.
                filters.put(key, value);
                opers.put(key, CompareOp.GREATER);
            } else if (oper.equals("greater_or_equal")) {
                // The value stored in the row or column must be greater than or
                // equal to the specified value.
                filters.put(key, value);
                opers.put(key, CompareOp.GREATER_OR_EQUAL);
            } else if (oper.equals("less")) {
                // The value stored in the row or column must be less than the
                // specified value.
                filters.put(key, value);
                opers.put(key, CompareOp.LESS);
            } else if (oper.equals("less_or_equal")) {
                // The value stored in the row or column must be less than or
                // equal to the specified value.
                filters.put(key, value);
                opers.put(key, CompareOp.LESS_OR_EQUAL);
            } else if (oper.equals("no_op")) {
                // No comparison operation is performed.
                filters.put(key, value);
                opers.put(key, CompareOp.NO_OP);
            } else {
                throw new RuntimeException("unkown operator: " + expression);
            }
        }
    }
}
