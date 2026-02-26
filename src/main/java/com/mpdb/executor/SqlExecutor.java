package com.mpdb.executor;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.springframework.stereotype.Component;

@Component
public class SqlExecutor {

    private final CreateTableHandler createTableHandler;
    private final InsertHandler insertHandler;
    private final SelectHandler selectHandler;
    private final DeleteHandler deleteHandler;
    private final UpdateHandler updateHandler;
    private final DropTableHandler dropTableHandler;

    public SqlExecutor(CreateTableHandler createTableHandler,
                       InsertHandler insertHandler,
                       SelectHandler selectHandler,
                       DeleteHandler deleteHandler,
                       UpdateHandler updateHandler,
                       DropTableHandler dropTableHandler) {
        this.createTableHandler = createTableHandler;
        this.insertHandler = insertHandler;
        this.selectHandler = selectHandler;
        this.deleteHandler = deleteHandler;
        this.updateHandler = updateHandler;
        this.dropTableHandler = dropTableHandler;
    }

    public String execute(SqlNode node) {
        SqlKind kind = node.getKind();

        return switch (kind) {
            case CREATE_TABLE -> createTableHandler.handle(node);
            case INSERT -> insertHandler.handle(node);
            case SELECT -> selectHandler.handle(node);
            case DELETE -> deleteHandler.handle(node);
            case UPDATE -> updateHandler.handle(node);
            case DROP_TABLE -> dropTableHandler.handle(node);
            default -> throw new UnsupportedOperationException("Unsupported SQL statement: " + kind);
        };
    }
}
