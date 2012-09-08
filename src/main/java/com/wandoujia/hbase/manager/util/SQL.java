package com.wandoujia.hbase.manager.util;

import java.util.List;

import org.apache.hadoop.hbase.filter.FilterList.Operator;

public class SQL {

    private String table;

    private List<String> fields;

    private List<Condition> conditions;

    private Operator operator;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public List<Condition> getConditions() {
        return conditions;
    }

    public void setConditions(List<Condition> conditions) {
        this.conditions = conditions;
    }

    public Operator getOperator() {
        return operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    @Override
    public String toString() {
        String strFields = "";
        String strConditions = "";
        if (fields != null) {
            for (String field: fields) {
                strFields += field + ",";
            }
        }
        if (conditions != null) {
            for (Condition c: conditions) {
                strConditions += c.toString() + ",";
            }
        }
        return "SQL [table=" + table + ", fields=[" + strFields
                + "], conditions=[" + strConditions + "], operator="
                + operator.toString() + "]";
    }
}
