package com.mpdb.repl;

import com.mpdb.executor.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Processor for handling SQL queries using Apache Calcite.
 */
@Component
@Slf4j
public class SqlQueryProcessor implements Processor {

    private final CalciteQueryParser queryParser;
    private final DbState dbState;
    private final SqlExecutor sqlExecutor;

    public SqlQueryProcessor(CalciteQueryParser queryParser, DbState dbState, SqlExecutor sqlExecutor) {
        this.queryParser = queryParser;
        this.dbState = dbState;
        this.sqlExecutor = sqlExecutor;
    }

    @Override
    public String process(String input) {
        return handleSqlQuery(input);
    }

    /**
     * Handle SQL query using Calcite parser.
     * Parses the query, builds AST, validates syntax, and executes.
     */
    private String handleSqlQuery(String sql) {
        CalciteQueryParser.ParseResult result = queryParser.parseAndValidate(sql);
        if (!result.isValid()) {
            return "SQL Parse Error:\n" + result.errorMessage();
        }

        if (dbState.isDebugAstMode()) {
            String queryType = result.getSqlKind();
            String astString = result.getAstString();
            System.out.printf("\nQuery Type: %s\nAST:\n%s\n", queryType, astString);
        }

        try {
            return sqlExecutor.execute(result.ast());
        } catch (Exception e) {
            return "Execution Error: " + e.getMessage();
        }
    }

}
