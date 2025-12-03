package com.mpdb.repl;

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

    public SqlQueryProcessor(CalciteQueryParser queryParser, DbState dbState) {
        this.queryParser = queryParser;
        this.dbState = dbState;
    }

    @Override
    public String process(String input) {
        return handleSqlQuery(input);
    }

    /**
     * Handle SQL query using Calcite parser.
     * Parses the query, builds AST, and validates syntax.
     */
    private String handleSqlQuery(String sql) {
        CalciteQueryParser.ParseResult result = queryParser.parseAndValidate(sql);
        if (!result.isValid()) {
            return "❌ SQL Parse Error:\n" + result.errorMessage();
        }

        if (dbState.isDebugAstMode()) {
            String queryType = result.getSqlKind();
            String astString = result.getAstString();
            System.out.printf("\nQuery Type: %s\nAST:\n%s\n", queryType, astString);
        }
        return "\n⚠️  Note: Query execution is not yet implemented.";
    }

}

