package com.mpdb.executor;

import com.mpdb.catalog.ColumnDefinition;
import com.mpdb.catalog.TableSchema;
import com.mpdb.storage.Tuple;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.springframework.stereotype.Component;

import java.util.function.Predicate;

@Component
public class PredicateBuilder {

    public Predicate<Tuple> build(SqlNode whereClause, TableSchema schema) {
        if (whereClause == null) {
            return t -> true;
        }

        if (whereClause instanceof SqlBasicCall call) {
            SqlOperator op = call.getOperator();

            // AND
            if (op == SqlStdOperatorTable.AND) {
                Predicate<Tuple> left = build(call.operand(0), schema);
                Predicate<Tuple> right = build(call.operand(1), schema);
                return left.and(right);
            }

            // OR
            if (op == SqlStdOperatorTable.OR) {
                Predicate<Tuple> left = build(call.operand(0), schema);
                Predicate<Tuple> right = build(call.operand(1), schema);
                return left.or(right);
            }

            // Comparison operators
            if (call.operandCount() == 2) {
                return buildComparison(call, schema);
            }
        }

        throw new UnsupportedOperationException("Unsupported WHERE clause: " + whereClause);
    }

    @SuppressWarnings("unchecked")
    private Predicate<Tuple> buildComparison(SqlBasicCall call, TableSchema schema) {
        SqlNode leftNode = call.operand(0);
        SqlNode rightNode = call.operand(1);
        SqlOperator op = call.getOperator();

        if (!(leftNode instanceof SqlIdentifier id)) {
            throw new UnsupportedOperationException("Left side of comparison must be a column name");
        }

        String columnName = id.getSimple();
        int colIndex = schema.getColumnIndex(columnName);
        if (colIndex < 0) {
            throw new IllegalArgumentException("Unknown column: " + columnName);
        }

        ColumnDefinition colDef = schema.getColumn(colIndex);
        Object literal = extractLiteral(rightNode, colDef);

        return tuple -> {
            Object value = tuple.getValue(colIndex);
            if (value == null || literal == null) {
                return false;
            }

            int cmp;
            if (value instanceof Integer iv && literal instanceof Integer il) {
                cmp = Integer.compare(iv, il);
            } else if (value instanceof String sv && literal instanceof String sl) {
                cmp = sv.compareTo(sl);
            } else if (value instanceof Float fv && literal instanceof Float fl) {
                cmp = Float.compare(fv, fl);
            } else if (value instanceof Boolean bv && literal instanceof Boolean bl) {
                cmp = Boolean.compare(bv, bl);
            } else {
                throw new IllegalStateException("Type mismatch in comparison");
            }

            if (op == SqlStdOperatorTable.EQUALS) return cmp == 0;
            if (op == SqlStdOperatorTable.NOT_EQUALS) return cmp != 0;
            if (op == SqlStdOperatorTable.LESS_THAN) return cmp < 0;
            if (op == SqlStdOperatorTable.LESS_THAN_OR_EQUAL) return cmp <= 0;
            if (op == SqlStdOperatorTable.GREATER_THAN) return cmp > 0;
            if (op == SqlStdOperatorTable.GREATER_THAN_OR_EQUAL) return cmp >= 0;
            throw new UnsupportedOperationException("Unsupported operator: " + op);
        };
    }

    private Object extractLiteral(SqlNode node, ColumnDefinition colDef) {
        if (node instanceof SqlNumericLiteral numLit) {
            if (colDef.type() == com.mpdb.catalog.ColumnType.FLOAT) {
                return numLit.bigDecimalValue().floatValue();
            }
            return numLit.intValue(true);
        }
        if (node instanceof SqlCharStringLiteral strLit) {
            return strLit.getNlsString().getValue();
        }
        if (node instanceof SqlLiteral lit) {
            // Handle boolean
            if (lit.getTypeName() == org.apache.calcite.sql.type.SqlTypeName.BOOLEAN) {
                return lit.booleanValue();
            }
        }
        throw new UnsupportedOperationException("Unsupported literal: " + node);
    }
}
