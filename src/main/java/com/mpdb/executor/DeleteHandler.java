package com.mpdb.executor;

import com.mpdb.catalog.Catalog;
import com.mpdb.catalog.TableSchema;
import com.mpdb.storage.HeapFile;
import com.mpdb.storage.StorageEngine;
import com.mpdb.storage.Tuple;
import com.mpdb.storage.TupleId;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

@Component
public class DeleteHandler implements StatementHandler {

    private final Catalog catalog;
    private final StorageEngine storageEngine;
    private final PredicateBuilder predicateBuilder;

    public DeleteHandler(Catalog catalog, StorageEngine storageEngine, PredicateBuilder predicateBuilder) {
        this.catalog = catalog;
        this.storageEngine = storageEngine;
        this.predicateBuilder = predicateBuilder;
    }

    @Override
    public String handle(SqlNode node) {
        SqlDelete delete = (SqlDelete) node;
        SqlNode targetTable = delete.getTargetTable();

        if (!(targetTable instanceof SqlIdentifier tableId)) {
            throw new UnsupportedOperationException("Only simple table references are supported");
        }

        String tableName = tableId.getSimple();
        TableSchema schema = catalog.getTable(tableName);
        if (schema == null) {
            throw new IllegalStateException("Table does not exist: " + tableName);
        }

        HeapFile heapFile = storageEngine.getHeapFile(tableName);
        SqlNode condition = delete.getCondition();

        List<Map.Entry<TupleId, Tuple>> candidates;
        if (condition != null) {
            Predicate<Tuple> predicate = predicateBuilder.build(condition, schema);
            candidates = heapFile.scanWithFilterAndIds(predicate);
        } else {
            candidates = heapFile.scanAllWithIds();
        }

        int deletedCount = 0;
        for (Map.Entry<TupleId, Tuple> entry : candidates) {
            if (heapFile.deleteTuple(entry.getKey())) {
                deletedCount++;
            }
        }

        return "Deleted " + deletedCount + (deletedCount == 1 ? " row." : " rows.");
    }
}
