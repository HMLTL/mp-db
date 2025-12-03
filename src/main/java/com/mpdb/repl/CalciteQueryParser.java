package com.mpdb.repl;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.springframework.stereotype.Component;

/**
 * SQL query parser using Apache Calcite.
 * Provides SQL parsing, AST generation, and basic validation.
 * Supports both DML (SELECT, INSERT, UPDATE, DELETE) and DDL (CREATE, DROP, ALTER) statements.
 */
@Component
public class CalciteQueryParser {

    private final SqlParser.Config parserConfig;

    public CalciteQueryParser() {
        // Configure SQL parser to support DDL statements using SqlDdlParserImpl
        this.parserConfig = SqlParser.config()
                .withCaseSensitive(false)
                .withParserFactory(SqlDdlParserImpl.FACTORY);
    }

    /**
     * Parse SQL query into Abstract Syntax Tree (AST).
     *
     * @param sql SQL query string
     * @return SqlNode representing the AST
     * @throws SqlParseException if SQL syntax is invalid
     */
    public SqlNode parse(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql, parserConfig);
        return parser.parseStmt();
    }

    /**
     * Parse and validate SQL query, returning a result object.
     *
     * @param sql SQL query string
     * @return ParseResult containing validation status, AST, and error message
     */
    public ParseResult parseAndValidate(String sql) {
        try {
            SqlNode sqlNode = parse(sql);
            return new ParseResult(true, sqlNode, null, sql);
        } catch (SqlParseException e) {
            return new ParseResult(false, null, formatError(e), sql);
        }
    }

    /**
     * Format parse exception into user-friendly error message.
     */
    private String formatError(SqlParseException e) {
        return String.format("Syntax error at line %d, column %d: %s",
                e.getPos().getLineNum(),
                e.getPos().getColumnNum(),
                e.getMessage());
    }

    /**
     * Result of SQL parsing operation.
     *
     * @param isValid      true if parsing succeeded
     * @param ast          Abstract Syntax Tree (SqlNode) if parsing succeeded
     * @param errorMessage error message if parsing failed
     * @param originalSql  original SQL query
     */
    public record ParseResult(
            boolean isValid,
            SqlNode ast,
            String errorMessage,
            String originalSql
    ) {
        /**
         * Get the AST as a formatted string.
         */
        public String getAstString() {
            return ast != null ? ast.toString() : "N/A";
        }

        /**
         * Get the SQL kind (SELECT, INSERT, etc.).
         */
        public String getSqlKind() {
            return ast != null ? ast.getKind().toString() : "N/A";
        }
    }
}

