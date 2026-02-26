package com.mpdb.catalog;

import java.util.Collections;
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
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equalsIgnoreCase(name)) {
                return i;
            }
        }
        return -1;
    }
}
