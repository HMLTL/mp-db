package com.mpdb.storage;

import com.mpdb.catalog.TableSchema;

public class Tuple {

    private final Object[] values;
    private final TableSchema schema;

    public Tuple(TableSchema schema, Object[] values) {
        this.schema = schema;
        this.values = values;
    }

    public Object getValue(int index) {
        return values[index];
    }

    public Object getValue(String columnName) {
        int index = schema.getColumnIndex(columnName);
        if (index < 0) {
            throw new IllegalArgumentException("Unknown column: " + columnName);
        }
        return values[index];
    }

    public Object[] getValues() {
        return values;
    }

    public TableSchema getSchema() {
        return schema;
    }

    public int getColumnCount() {
        return values.length;
    }

    public static Tuple merge(Tuple left, Tuple right, TableSchema mergedSchema) {
        Object[] merged = new Object[left.values.length + right.values.length];
        System.arraycopy(left.values, 0, merged, 0, left.values.length);
        System.arraycopy(right.values, 0, merged, left.values.length, right.values.length);
        return new Tuple(mergedSchema, merged);
    }
}
