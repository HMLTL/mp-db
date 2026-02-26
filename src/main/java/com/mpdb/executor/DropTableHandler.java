package com.mpdb.executor;

import com.mpdb.catalog.Catalog;
import com.mpdb.storage.StorageEngine;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.springframework.stereotype.Component;

@Component
public class DropTableHandler implements StatementHandler {

    private final Catalog catalog;
    private final StorageEngine storageEngine;

    public DropTableHandler(Catalog catalog, StorageEngine storageEngine) {
        this.catalog = catalog;
        this.storageEngine = storageEngine;
    }

    @Override
    public String handle(SqlNode node) {
        SqlDropTable dropTable = (SqlDropTable) node;
        String tableName = dropTable.name.getSimple();

        if (!catalog.tableExists(tableName)) {
            throw new IllegalStateException("Table does not exist: " + tableName);
        }

        storageEngine.dropHeapFile(tableName);
        catalog.dropTable(tableName);

        return "Table '" + tableName + "' dropped.";
    }
}
