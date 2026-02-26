package com.mpdb.executor;

import com.mpdb.catalog.*;
import com.mpdb.storage.StorageEngine;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class CreateTableHandler implements StatementHandler {

    private final Catalog catalog;
    private final StorageEngine storageEngine;

    public CreateTableHandler(Catalog catalog, StorageEngine storageEngine) {
        this.catalog = catalog;
        this.storageEngine = storageEngine;
    }

    @Override
    public String handle(SqlNode node) {
        SqlCreateTable createTable = (SqlCreateTable) node;
        String tableName = createTable.name.getSimple();

        if (catalog.tableExists(tableName)) {
            throw new IllegalStateException("Table already exists: " + tableName);
        }

        List<ColumnDefinition> columns = new ArrayList<>();
        for (SqlNode colNode : createTable.columnList) {
            if (colNode instanceof SqlColumnDeclaration colDecl) {
                String colName = colDecl.name.getSimple();
                SqlDataTypeSpec typeSpec = colDecl.dataType;
                String typeName = typeSpec.getTypeName().getSimple().toUpperCase();

                ColumnType colType;
                int maxLength = 0;

                switch (typeName) {
                    case "INT", "INTEGER" -> colType = ColumnType.INT;
                    case "VARCHAR" -> {
                        colType = ColumnType.VARCHAR;
                        maxLength = typeSpec.getTypeName().getSimple().length(); // default
                        // Try to extract precision from type spec
                        if (typeSpec.toString().contains("(")) {
                            String spec = typeSpec.toString();
                            int start = spec.indexOf('(') + 1;
                            int end = spec.indexOf(')');
                            if (start > 0 && end > start) {
                                maxLength = Integer.parseInt(spec.substring(start, end).trim());
                            }
                        }
                        if (maxLength <= 0) maxLength = 255;
                    }
                    case "FLOAT", "REAL" -> colType = ColumnType.FLOAT;
                    case "TEXT" -> {
                        colType = ColumnType.TEXT;
                        maxLength = 0;
                    }
                    case "BOOLEAN", "BOOL" -> colType = ColumnType.BOOLEAN;
                    default -> throw new UnsupportedOperationException("Unsupported column type: " + typeName);
                }

                columns.add(new ColumnDefinition(colName, colType, maxLength));
            }
        }

        TableSchema schema = new TableSchema(tableName, columns);
        catalog.createTable(schema);
        storageEngine.createHeapFile(schema);

        return "Table '" + tableName + "' created.";
    }
}
