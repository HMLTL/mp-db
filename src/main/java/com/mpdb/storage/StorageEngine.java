package com.mpdb.storage;

import com.mpdb.catalog.TableSchema;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class StorageEngine {

    private final ConcurrentHashMap<String, HeapFile> heapFiles = new ConcurrentHashMap<>();

    public HeapFile createHeapFile(TableSchema schema) {
        String key = schema.getTableName().toUpperCase();
        if (heapFiles.containsKey(key)) {
            throw new IllegalStateException("Heap file already exists for table: " + schema.getTableName());
        }
        HeapFile heapFile = new HeapFile(schema);
        heapFiles.put(key, heapFile);
        return heapFile;
    }

    public HeapFile getHeapFile(String tableName) {
        return heapFiles.get(tableName.toUpperCase());
    }

    public void dropHeapFile(String tableName) {
        String key = tableName.toUpperCase();
        if (heapFiles.remove(key) == null) {
            throw new IllegalStateException("No heap file for table: " + tableName);
        }
    }

    public boolean heapFileExists(String tableName) {
        return heapFiles.containsKey(tableName.toUpperCase());
    }
}
