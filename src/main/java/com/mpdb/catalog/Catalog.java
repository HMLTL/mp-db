package com.mpdb.catalog;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class Catalog {

    private final ConcurrentHashMap<String, TableSchema> tables = new ConcurrentHashMap<>();

    public void createTable(TableSchema schema) {
        String key = schema.getTableName().toUpperCase();
        if (tables.containsKey(key)) {
            throw new IllegalStateException("Table already exists: " + schema.getTableName());
        }
        tables.put(key, schema);
    }

    public void dropTable(String tableName) {
        String key = tableName.toUpperCase();
        if (tables.remove(key) == null) {
            throw new IllegalStateException("Table does not exist: " + tableName);
        }
    }

    public TableSchema getTable(String tableName) {
        return tables.get(tableName.toUpperCase());
    }

    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName.toUpperCase());
    }
}
