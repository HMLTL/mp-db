package com.mpdb.executor;

import com.mpdb.catalog.Catalog;
import com.mpdb.catalog.TableSchema;
import com.mpdb.storage.HeapFile;
import com.mpdb.storage.StorageEngine;
import com.mpdb.storage.Tuple;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.Predicate;

@Component
public class SelectHandler implements StatementHandler {

    private final Catalog catalog;
    private final StorageEngine storageEngine;
    private final PredicateBuilder predicateBuilder;

    public SelectHandler(Catalog catalog, StorageEngine storageEngine, PredicateBuilder predicateBuilder) {
        this.catalog = catalog;
        this.storageEngine = storageEngine;
        this.predicateBuilder = predicateBuilder;
    }

    @Override
    public String handle(SqlNode node) {
        SqlSelect select = (SqlSelect) node;
        SqlNode from = select.getFrom();

        if (!(from instanceof SqlIdentifier tableId)) {
            throw new UnsupportedOperationException("Only simple table references are supported in FROM");
        }

        String tableName = tableId.getSimple();
        TableSchema schema = catalog.getTable(tableName);
        if (schema == null) {
            throw new IllegalStateException("Table does not exist: " + tableName);
        }

        HeapFile heapFile = storageEngine.getHeapFile(tableName);
        SqlNode where = select.getWhere();

        List<Tuple> results;
        if (where != null) {
            Predicate<Tuple> predicate = predicateBuilder.build(where, schema);
            results = heapFile.scanWithFilter(predicate);
        } else {
            results = heapFile.scanAll();
        }

        return ResultFormatter.format(results, schema);
    }
}
