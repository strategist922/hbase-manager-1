package com.wandoujia.hbase.manager.util;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

public class Condition {

    private String field;

    private SQLCompareOp sqlCompareOp;

    private String value;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public SQLCompareOp getSqlCompareOp() {
        return sqlCompareOp;
    }

    public void setSqlCompareOp(SQLCompareOp sqlCompareOp) {
        this.sqlCompareOp = sqlCompareOp;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public static CompareOp getHBaseCompareOp(SQLCompareOp operator) {
        if (operator.getValue().equals(SQLCompareOp.EQUAL.getValue())) {
            return CompareOp.EQUAL;
        } else if (operator.getValue()
                .equals(SQLCompareOp.NOT_EQUAL.getValue())) {
            return CompareOp.NOT_EQUAL;
        } else if (operator.getValue().equals(SQLCompareOp.GREATER.getValue())) {
            return CompareOp.GREATER;
        } else if (operator.getValue().equals(
                SQLCompareOp.GREATER_OR_EQUAL.getValue())) {
            return CompareOp.GREATER_OR_EQUAL;
        } else if (operator.getValue().equals(SQLCompareOp.LESS.getValue())) {
            return CompareOp.LESS;
        } else if (operator.getValue().equals(
                SQLCompareOp.LESS_OR_EQUAL.getValue())) {
            return CompareOp.LESS_OR_EQUAL;
        } else {
            return null;
        }
    }

    public enum SQLCompareOp {
        EQUAL("="), NOT_EQUAL("!="), LIKE("like"), GREATER_OR_EQUAL(">="), LESS_OR_EQUAL(
                "<="), GREATER(">"), LESS("<");

        private final String value;

        SQLCompareOp(String value) {
            this.value = value;
        }

        public String getValue() {
            return this.value;
        }

        public static SQLCompareOp parse(String value) {
            for (SQLCompareOp period: SQLCompareOp.values()) {
                if (period.value.equals(value)) {
                    return period;
                }
            }
            return null;
        }
    }

    @Override
    public String toString() {
        return "Condition [field=" + field + ", operator=" + sqlCompareOp
                + ", value=" + value + "]";
    }

}
