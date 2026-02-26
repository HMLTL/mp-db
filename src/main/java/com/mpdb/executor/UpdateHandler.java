package com.mpdb.executor;

import com.mpdb.catalog.Catalog;
import com.mpdb.catalog.ColumnDefinition;
import com.mpdb.catalog.ColumnType;
import com.mpdb.catalog.TableSchema;
import com.mpdb.storage.HeapFile;
import com.mpdb.storage.StorageEngine;
import com.mpdb.storage.Tuple;
import com.mpdb.storage.TupleId;
import org.apache.calcite.sql.*;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

@Component
public class UpdateHandler implements StatementHandler {

    private final Catalog catalog;
    private final StorageEngine storageEngine;
    private final PredicateBuilder predicateBuilder;

    public UpdateHandler(Catalog catalog, StorageEngine storageEngine, PredicateBuilder predicateBuilder) {
        this.catalog = catalog;
        this.storageEngine = storageEngine;
        this.predicateBuilder = predicateBuilder;
    }

    @Override
    public String handle(SqlNode node) {
        SqlUpdate update = (SqlUpdate) node;
        SqlNode targetTable = update.getTargetTable();

        if (!(targetTable instanceof SqlIdentifier tableId)) {
            throw new UnsupportedOperationException("Only simple table references are supported");
        }

        String tableName = tableId.getSimple();
        TableSchema schema = catalog.getTable(tableName);
        if (schema == null) {
            throw new IllegalStateException("Table does not exist: " + tableName);
        }

        HeapFile heapFile = storageEngine.getHeapFile(tableName);
        SqlNode condition = update.getCondition();

        List<Map.Entry<TupleId, Tuple>> candidates;
        if (condition != null) {
            Predicate<Tuple> predicate = predicateBuilder.build(condition, schema);
            candidates = heapFile.scanWithFilterAndIds(predicate);
        } else {
            candidates = heapFile.scanAllWithIds();
        }

        // Build SET assignments: column index -> new value
        SqlNodeList targetColumns = update.getTargetColumnList();
        SqlNodeList sourceExpressions = update.getSourceExpressionList();

        int[] updateColIndices = new int[targetColumns.size()];
        Object[] updateValues = new Object[targetColumns.size()];

        for (int i = 0; i < targetColumns.size(); i++) {
            SqlIdentifier colId = (SqlIdentifier) targetColumns.get(i);
            String colName = colId.getSimple();
            int colIndex = schema.getColumnIndex(colName);
            if (colIndex < 0) {
                throw new IllegalArgumentException("Unknown column: " + colName);
            }
            updateColIndices[i] = colIndex;
            ColumnDefinition colDef = schema.getColumn(colIndex);
            updateValues[i] = extractValue(sourceExpressions.get(i), colDef);
        }

        int updatedCount = 0;
        for (Map.Entry<TupleId, Tuple> entry : candidates) {
            Tuple oldTuple = entry.getValue();
            Object[] newValues = new Object[schema.getColumnCount()];
            for (int c = 0; c < schema.getColumnCount(); c++) {
                newValues[c] = oldTuple.getValue(c);
            }
            for (int i = 0; i < updateColIndices.length; i++) {
                newValues[updateColIndices[i]] = updateValues[i];
            }

            // Delete old, insert new (handles variable-length size changes)
            heapFile.deleteTuple(entry.getKey());
            heapFile.insertTuple(new Tuple(schema, newValues));
            updatedCount++;
        }

        return "Updated " + updatedCount + (updatedCount == 1 ? " row." : " rows.");
    }

    private Object extractValue(SqlNode node, ColumnDefinition colDef) {
        if (node instanceof SqlNumericLiteral numLit) {
            if (colDef.type() == ColumnType.FLOAT) {
                return numLit.bigDecimalValue().floatValue();
            }
            return numLit.intValue(true);
        }
        if (node instanceof SqlCharStringLiteral strLit) {
            return strLit.getNlsString().getValue();
        }
        if (node instanceof SqlLiteral lit) {
            if (lit.getTypeName() == org.apache.calcite.sql.type.SqlTypeName.BOOLEAN) {
                return lit.booleanValue();
            }
        }
        throw new UnsupportedOperationException("Unsupported value: " + node);
    }
}