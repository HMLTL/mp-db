package com.mpdb.storage;

import com.mpdb.catalog.Catalog;
import com.mpdb.catalog.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class StorageEngine {

    private static final Logger log = LoggerFactory.getLogger(StorageEngine.class);

    private final ConcurrentHashMap<String, HeapFile> heapFiles = new ConcurrentHashMap<>();
    private final Path dataDir;
    private final Catalog catalog;

    public StorageEngine(@Value("${app.data-dir:./data}") String dataDir, Catalog catalog) {
        this.dataDir = Path.of(dataDir);
        this.catalog = catalog;
    }

    @PostConstruct
    public void init() {
        if (catalog == null) return;
        // Reload heap files for all tables in the catalog
        for (TableSchema schema : catalog.getAllTables()) {
            String key = schema.getTableName().toUpperCase();
            try {
                DiskPageManager diskManager = new DiskPageManager(heapFilePath(schema.getTableName()));
                HeapFile heapFile = new HeapFile(schema, diskManager);
                heapFiles.put(key, heapFile);
                log.info("Restored heap file for table '{}'", schema.getTableName());
            } catch (IOException e) {
                log.error("Failed to restore heap file for table '{}': {}", schema.getTableName(), e.getMessage());
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        for (HeapFile heapFile : heapFiles.values()) {
            heapFile.close();
        }
        log.info("Closed all heap files.");
    }

    public HeapFile createHeapFile(TableSchema schema) {
        String key = schema.getTableName().toUpperCase();
        if (heapFiles.containsKey(key)) {
            throw new IllegalStateException("Heap file already exists for table: " + schema.getTableName());
        }
        HeapFile heapFile;
        if (dataDir != null) {
            try {
                DiskPageManager diskManager = new DiskPageManager(heapFilePath(schema.getTableName()));
                heapFile = new HeapFile(schema, diskManager);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to create heap file for table: " + schema.getTableName(), e);
            }
        } else {
            heapFile = new HeapFile(schema);
        }
        heapFiles.put(key, heapFile);
        return heapFile;
    }

    public HeapFile getHeapFile(String tableName) {
        return heapFiles.get(tableName.toUpperCase());
    }

    public void dropHeapFile(String tableName) {
        String key = tableName.toUpperCase();
        HeapFile heapFile = heapFiles.remove(key);
        if (heapFile == null) {
            throw new IllegalStateException("No heap file for table: " + tableName);
        }
        heapFile.deleteFiles();
    }

    public boolean heapFileExists(String tableName) {
        return heapFiles.containsKey(tableName.toUpperCase());
    }

    private Path heapFilePath(String tableName) {
        return dataDir.resolve(tableName.toUpperCase() + ".dat");
    }
}
