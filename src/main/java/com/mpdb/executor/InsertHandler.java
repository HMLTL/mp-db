package com.mpdb.executor;

import com.mpdb.catalog.Catalog;
import com.mpdb.catalog.ColumnDefinition;
import com.mpdb.catalog.TableSchema;
import com.mpdb.storage.HeapFile;
import com.mpdb.storage.StorageEngine;
import com.mpdb.storage.Tuple;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.springframework.stereotype.Component;

@Component
public class InsertHandler implements StatementHandler {

    private final Catalog catalog;
    private final StorageEngine storageEngine;

    public InsertHandler(Catalog catalog, StorageEngine storageEngine) {
        this.catalog = catalog;
        this.storageEngine = storageEngine;
    }

    @Override
    public String handle(SqlNode node) {
        SqlInsert insert = (SqlInsert) node;
        String tableName = ((SqlIdentifier) insert.getTargetTable()).getSimple();

        TableSchema schema = catalog.getTable(tableName);
        if (schema == null) {
            throw new IllegalStateException("Table does not exist: " + tableName);
        }

        HeapFile heapFile = storageEngine.getHeapFile(tableName);
        SqlNode source = insert.getSource();

        int rowCount = 0;

        if (source instanceof SqlBasicCall valuesCall && valuesCall.getOperator() == SqlStdOperatorTable.VALUES) {
            for (SqlNode rowNode : valuesCall.getOperandList()) {
                SqlBasicCall row = (SqlBasicCall) rowNode;
                Object[] values = new Object[schema.getColumnCount()];

                for (int i = 0; i < row.operandCount(); i++) {
                    ColumnDefinition colDef = schema.getColumn(i);
                    values[i] = extractValue(row.operand(i), colDef);
                }

                heapFile.insertTuple(new Tuple(schema, values));
                rowCount++;
            }
        } else {
            throw new UnsupportedOperationException("Only INSERT INTO ... VALUES is supported");
        }

        return "Inserted " + rowCount + (rowCount == 1 ? " row." : " rows.");
    }

    private Object extractValue(SqlNode node, ColumnDefinition colDef) {
        if (node instanceof SqlLiteral lit
                && lit.getTypeName() == org.apache.calcite.sql.type.SqlTypeName.NULL) {
            return null;
        }
        if (node instanceof SqlNumericLiteral numLit) {
            if (colDef.type() == com.mpdb.catalog.ColumnType.FLOAT) {
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
