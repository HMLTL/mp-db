package com.mpdb.executor;

import org.apache.calcite.sql.SqlNode;

public interface StatementHandler {
    String handle(SqlNode node);
}
