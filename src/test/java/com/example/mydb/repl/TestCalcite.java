package com.example.mydb.repl;

public class TestCalcite {
    public static void main(String[] args) {
        CalciteQueryParser parser = new CalciteQueryParser();

        // Test CREATE TABLE
        String sql = "CREATE TABLE users (id INT, name VARCHAR(50))";
        CalciteQueryParser.ParseResult result = parser.parseAndValidate(sql);

        System.out.println("SQL: " + sql);
        System.out.println("Is valid: " + result.isValid());
        System.out.println("Error: " + result.errorMessage());
        System.out.println("SQL Kind: " + result.getSqlKind());
    }
}

