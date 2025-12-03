package com.example.mydb.repl;

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

        // Build response based on debug modes
        StringBuilder response = new StringBuilder();

        if (dbState.isDebugAstMode()) {
            String queryType = result.getSqlKind();
            String formattedAst = formatAst(result.getAstString());

            log.debug("Query Type: {}", queryType);
            log.debug("AST:\n{}", formattedAst);

            response.append("Query Type: ").append(queryType).append("\n");
            response.append("\nAST:\n").append(formattedAst).append("\n");
            response.append("\n⚠️  Note: Query execution is not yet implemented.");
        }

        return response.toString();
    }

    /**
     * Format AST string for better readability.
     */
    private String formatAst(String ast) {
        // Add indentation for better readability
        return ast.replace(", ", ",\n  ")
                  .replace("(", "(\n  ")
                  .replace(")", "\n)");
    }
}

