package com.mpdb.catalog;

import java.util.ArrayList;
import java.util.List;

public class TableSchema {

    private final String tableName;
    private final List<ColumnDefinition> columns;

    public TableSchema(String tableName, List<ColumnDefinition> columns) {
        this.tableName = tableName;
        this.columns = List.copyOf(columns);
    }

    public String getTableName() {
        return tableName;
    }

    public List<ColumnDefinition> getColumns() {
        return columns;
    }

    public int getColumnCount() {
        return columns.size();
    }

    public ColumnDefinition getColumn(int index) {
        return columns.get(index);
    }

    public ColumnDefinition getColumn(String name) {
        for (ColumnDefinition col : columns) {
            if (col.name().equalsIgnoreCase(name)) {
                return col;
            }
        }
        return null;
    }

    public int getColumnIndex(String name) {
        // Exact match first (handles both simple and qualified names)
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equalsIgnoreCase(name)) {
                return i;
            }
        }

        // Suffix match: bare column name matches "alias.col"
        int found = -1;
        String suffix = "." + name.toUpperCase();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().toUpperCase().endsWith(suffix)) {
                if (found >= 0) {
                    throw new IllegalArgumentException("Ambiguous column reference: " + name);
                }
                found = i;
            }
        }
        return found;
    }

    public static TableSchema merge(String leftAlias, TableSchema left, String rightAlias, TableSchema right) {
        List<ColumnDefinition> merged = new ArrayList<>();
        for (ColumnDefinition col : left.getColumns()) {
            merged.add(new ColumnDefinition(leftAlias + "." + col.name(), col.type(), col.maxLength()));
        }
        for (ColumnDefinition col : right.getColumns()) {
            merged.add(new ColumnDefinition(rightAlias + "." + col.name(), col.type(), col.maxLength()));
        }
        return new TableSchema(leftAlias + "_" + rightAlias, merged);
    }
}
