package com.mpdb.catalog;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Persists catalog metadata (table schemas) to a flat file.
 * Format per table:
 *   TABLE tableName
 *   COLUMN name type maxLength
 *   ...
 *   END
 */
public class CatalogPersistence {

    private final Path catalogFile;

    public CatalogPersistence(Path dataDir) {
        this.catalogFile = dataDir.resolve("catalog.meta");
    }

    public void save(ConcurrentHashMap<String, TableSchema> tables) throws IOException {
        Files.createDirectories(catalogFile.getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(catalogFile)) {
            for (Map.Entry<String, TableSchema> entry : tables.entrySet()) {
                TableSchema schema = entry.getValue();
                writer.write("TABLE " + schema.getTableName());
                writer.newLine();
                for (ColumnDefinition col : schema.getColumns()) {
                    writer.write("COLUMN " + col.name() + " " + col.type().name() + " " + col.maxLength());
                    writer.newLine();
                }
                writer.write("END");
                writer.newLine();
            }
        }
    }

    public ConcurrentHashMap<String, TableSchema> load() throws IOException {
        ConcurrentHashMap<String, TableSchema> tables = new ConcurrentHashMap<>();
        if (!Files.exists(catalogFile)) {
            return tables;
        }

        try (BufferedReader reader = Files.newBufferedReader(catalogFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("TABLE ")) {
                    String tableName = line.substring(6).trim();
                    List<ColumnDefinition> columns = new ArrayList<>();
                    String colLine;
                    while ((colLine = reader.readLine()) != null) {
                        colLine = colLine.trim();
                        if (colLine.equals("END")) break;
                        if (colLine.startsWith("COLUMN ")) {
                            String[] parts = colLine.substring(7).split(" ");
                            String colName = parts[0];
                            ColumnType colType = ColumnType.valueOf(parts[1]);
                            int maxLength = Integer.parseInt(parts[2]);
                            columns.add(new ColumnDefinition(colName, colType, maxLength));
                        }
                    }
                    TableSchema schema = new TableSchema(tableName, columns);
                    tables.put(tableName.toUpperCase(), schema);
                }
            }
        }
        return tables;
    }
}
