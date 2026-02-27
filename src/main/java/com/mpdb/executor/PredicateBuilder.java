package com.mpdb.executor;

import com.mpdb.catalog.ColumnDefinition;
import com.mpdb.catalog.TableSchema;
import com.mpdb.storage.Tuple;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

@Component
public class PredicateBuilder {

    public Predicate<Tuple> build(SqlNode whereClause, TableSchema schema) {
        return build(whereClause, schema, null);
    }

    public Predicate<Tuple> build(SqlNode whereClause, TableSchema schema,
                                   Function<SqlSelect, List<Tuple>> subqueryExecutor) {
        if (whereClause == null) {
            return t -> true;
        }

        if (whereClause instanceof SqlBasicCall call) {
            SqlOperator op = call.getOperator();

            // AND
            if (op == SqlStdOperatorTable.AND) {
                Predicate<Tuple> left = build(call.operand(0), schema, subqueryExecutor);
                Predicate<Tuple> right = build(call.operand(1), schema, subqueryExecutor);
                return left.and(right);
            }

            // OR
            if (op == SqlStdOperatorTable.OR) {
                Predicate<Tuple> left = build(call.operand(0), schema, subqueryExecutor);
                Predicate<Tuple> right = build(call.operand(1), schema, subqueryExecutor);
                return left.or(right);
            }

            // IS NULL (postfix, 1 operand)
            if (op == SqlStdOperatorTable.IS_NULL) {
                int colIndex = resolveColumnIndex(call.operand(0), schema);
                return tuple -> tuple.getValue(colIndex) == null;
            }

            // IS NOT NULL (postfix, 1 operand)
            if (op == SqlStdOperatorTable.IS_NOT_NULL) {
                int colIndex = resolveColumnIndex(call.operand(0), schema);
                return tuple -> tuple.getValue(colIndex) != null;
            }

            // IN operator
            if (op == SqlStdOperatorTable.IN) {
                return buildIn(call, schema, subqueryExecutor);
            }

            // Comparison operators
            if (call.operandCount() == 2) {
                return buildComparison(call, schema);
            }
        }

        throw new UnsupportedOperationException("Unsupported WHERE clause: " + whereClause);
    }

    private int resolveColumnIndex(SqlNode node, TableSchema schema) {
        if (!(node instanceof SqlIdentifier id)) {
            throw new UnsupportedOperationException("Expected column identifier but got: " + node);
        }
        String columnName = resolveColumnName(id);
        int colIndex = schema.getColumnIndex(columnName);
        if (colIndex < 0) {
            throw new IllegalArgumentException("Unknown column: " + columnName);
        }
        return colIndex;
    }

    private String resolveColumnName(SqlIdentifier id) {
        if (id.names.size() == 2) {
            return id.names.get(0) + "." + id.names.get(1);
        }
        return id.getSimple();
    }

    private Predicate<Tuple> buildIn(SqlBasicCall call, TableSchema schema,
                                      Function<SqlSelect, List<Tuple>> subqueryExecutor) {
        int colIndex = resolveColumnIndex(call.operand(0), schema);
        SqlNode rightNode = call.operand(1);

        if (rightNode instanceof SqlSelect subquery) {
            if (subqueryExecutor == null) {
                throw new UnsupportedOperationException("Subquery execution not supported in this context");
            }
            List<Tuple> subResults = subqueryExecutor.apply(subquery);
            Set<Object> values = new java.util.HashSet<>();
            for (Tuple t : subResults) {
                values.add(t.getValue(0));
            }
            return tuple -> {
                Object val = tuple.getValue(colIndex);
                return val != null && values.contains(val);
            };
        }

        throw new UnsupportedOperationException("Unsupported IN clause: " + rightNode);
    }

    @SuppressWarnings("unchecked")
    private Predicate<Tuple> buildComparison(SqlBasicCall call, TableSchema schema) {
        SqlNode leftNode = call.operand(0);
        SqlNode rightNode = call.operand(1);
        SqlOperator op = call.getOperator();

        // Column = Column comparison (for JOIN ON clauses)
        if (leftNode instanceof SqlIdentifier leftId && rightNode instanceof SqlIdentifier rightId) {
            String leftCol = resolveColumnName(leftId);
            String rightCol = resolveColumnName(rightId);
            int leftIndex = schema.getColumnIndex(leftCol);
            int rightIndex = schema.getColumnIndex(rightCol);

            if (leftIndex >= 0 && rightIndex >= 0) {
                return tuple -> {
                    Object leftVal = tuple.getValue(leftIndex);
                    Object rightVal = tuple.getValue(rightIndex);
                    if (leftVal == null || rightVal == null) return false;
                    int cmp = compareValues(leftVal, rightVal);
                    return evalOp(op, cmp);
                };
            }
        }

        if (!(leftNode instanceof SqlIdentifier id)) {
            throw new UnsupportedOperationException("Left side of comparison must be a column name");
        }

        String columnName = resolveColumnName(id);
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
            int cmp = compareValues(value, literal);
            return evalOp(op, cmp);
        };
    }

    private int compareValues(Object a, Object b) {
        if (a instanceof Integer iv && b instanceof Integer ib) {
            return Integer.compare(iv, ib);
        } else if (a instanceof String sv && b instanceof String sb) {
            return sv.compareTo(sb);
        } else if (a instanceof Float fv && b instanceof Float fb) {
            return Float.compare(fv, fb);
        } else if (a instanceof Boolean bv && b instanceof Boolean bb) {
            return Boolean.compare(bv, bb);
        }
        throw new IllegalStateException("Type mismatch in comparison");
    }

    private boolean evalOp(SqlOperator op, int cmp) {
        if (op == SqlStdOperatorTable.EQUALS) return cmp == 0;
        if (op == SqlStdOperatorTable.NOT_EQUALS) return cmp != 0;
        if (op == SqlStdOperatorTable.LESS_THAN) return cmp < 0;
        if (op == SqlStdOperatorTable.LESS_THAN_OR_EQUAL) return cmp <= 0;
        if (op == SqlStdOperatorTable.GREATER_THAN) return cmp > 0;
        if (op == SqlStdOperatorTable.GREATER_THAN_OR_EQUAL) return cmp >= 0;
        throw new UnsupportedOperationException("Unsupported operator: " + op);
    }

    private Object extractLiteral(SqlNode node, ColumnDefinition colDef) {
        if (node instanceof SqlLiteral lit
                && lit.getTypeName() == org.apache.calcite.sql.type.SqlTypeName.NULL) {
            return null;
        }
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
