package com.mpdb.catalog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class Catalog {

    private static final Logger log = LoggerFactory.getLogger(Catalog.class);

    private ConcurrentHashMap<String, TableSchema> tables = new ConcurrentHashMap<>();
    private final CatalogPersistence persistence;

    public Catalog(@Value("${app.data-dir:./data}") String dataDir) {
        this.persistence = new CatalogPersistence(Path.of(dataDir));
    }

    // Package-private constructor for tests (no persistence)
    Catalog(CatalogPersistence persistence) {
        this.persistence = persistence;
    }

    @PostConstruct
    public void init() {
        try {
            tables = persistence.load();
            if (!tables.isEmpty()) {
                log.info("Loaded {} table(s) from catalog.", tables.size());
            }
        } catch (IOException e) {
            log.warn("Failed to load catalog: {}", e.getMessage());
        }
    }

    public void createTable(TableSchema schema) {
        String key = schema.getTableName().toUpperCase();
        if (tables.containsKey(key)) {
            throw new IllegalStateException("Table already exists: " + schema.getTableName());
        }
        tables.put(key, schema);
        flush();
    }

    public void dropTable(String tableName) {
        String key = tableName.toUpperCase();
        if (tables.remove(key) == null) {
            throw new IllegalStateException("Table does not exist: " + tableName);
        }
        flush();
    }

    public TableSchema getTable(String tableName) {
        return tables.get(tableName.toUpperCase());
    }

    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName.toUpperCase());
    }

    public Collection<TableSchema> getAllTables() {
        return tables.values();
    }

    private void flush() {
        try {
            persistence.save(tables);
        } catch (IOException e) {
            log.error("Failed to persist catalog: {}", e.getMessage());
        }
    }
}
