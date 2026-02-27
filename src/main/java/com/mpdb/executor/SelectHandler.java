package com.mpdb.executor;

import com.mpdb.catalog.Catalog;
import com.mpdb.catalog.ColumnDefinition;
import com.mpdb.catalog.TableSchema;
import com.mpdb.storage.HeapFile;
import com.mpdb.storage.StorageEngine;
import com.mpdb.storage.Tuple;
import org.apache.calcite.sql.*;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

@Component
public class SelectHandler implements StatementHandler {

    private final Catalog catalog;
    private final StorageEngine storageEngine;
    private final PredicateBuilder predicateBuilder;

    public SelectHandler(Catalog catalog, StorageEngine storageEngine, PredicateBuilder predicateBuilder) {
        this.catalog = catalog;
        this.storageEngine = storageEngine;
        this.predicateBuilder = predicateBuilder;
    }

    @Override
    public String handle(SqlNode node) {
        SqlSelect select = (SqlSelect) node;
        ResolvedRelation relation = resolveFrom(select.getFrom());
        List<Tuple> results = relation.tuples;
        TableSchema schema = relation.schema;

        // Apply WHERE filter
        SqlNode where = select.getWhere();
        if (where != null) {
            Predicate<Tuple> predicate = predicateBuilder.build(where, schema, this::executeSubSelect);
            results = results.stream().filter(predicate).toList();
        }

        // Apply column projection
        SqlNodeList selectList = select.getSelectList();
        if (selectList != null && !isSelectStar(selectList)) {
            ProjectionResult projection = applyProjection(selectList, results, schema);
            results = projection.tuples;
            schema = projection.schema;
        }

        return ResultFormatter.format(results, schema);
    }

    private List<Tuple> executeSubSelect(SqlSelect subSelect) {
        ResolvedRelation rel = resolveFrom(subSelect.getFrom());
        List<Tuple> results = rel.tuples;
        TableSchema schema = rel.schema;

        SqlNode where = subSelect.getWhere();
        if (where != null) {
            Predicate<Tuple> predicate = predicateBuilder.build(where, schema, this::executeSubSelect);
            results = results.stream().filter(predicate).toList();
        }

        SqlNodeList selectList = subSelect.getSelectList();
        if (selectList != null && !isSelectStar(selectList)) {
            ProjectionResult projection = applyProjection(selectList, results, schema);
            results = projection.tuples;
            schema = projection.schema;
        }

        return results;
    }

    private ResolvedRelation resolveFrom(SqlNode from) {
        // Simple table reference
        if (from instanceof SqlIdentifier tableId) {
            String tableName = tableId.getSimple();
            TableSchema schema = catalog.getTable(tableName);
            if (schema == null) {
                throw new IllegalStateException("Table does not exist: " + tableName);
            }
            HeapFile heapFile = storageEngine.getHeapFile(tableName);
            return new ResolvedRelation(heapFile.scanAll(), schema);
        }

        // JOIN
        if (from instanceof SqlJoin join) {
            return resolveJoin(join);
        }

        // AS alias (subquery or table alias)
        if (from instanceof SqlBasicCall call && call.getOperator().getKind() == SqlKind.AS) {
            SqlNode operand = call.operand(0);
            SqlIdentifier alias = call.operand(1);
            String aliasName = alias.getSimple();

            if (operand instanceof SqlSelect subSelect) {
                List<Tuple> subResults = executeSubSelect(subSelect);
                // Rebind tuples to aliased schema
                TableSchema subSchema;
                if (!subResults.isEmpty()) {
                    subSchema = subResults.get(0).getSchema();
                } else {
                    // Execute to get schema even with no results
                    ResolvedRelation rel = resolveFrom(subSelect.getFrom());
                    subSchema = rel.schema;
                }
                TableSchema aliasedSchema = new TableSchema(aliasName, subSchema.getColumns());
                List<Tuple> aliasedTuples = new ArrayList<>();
                for (Tuple t : subResults) {
                    aliasedTuples.add(new Tuple(aliasedSchema, t.getValues()));
                }
                return new ResolvedRelation(aliasedTuples, aliasedSchema);
            }

            // Table alias: `FROM users AS u`
            if (operand instanceof SqlIdentifier tableId) {
                String tableName = tableId.getSimple();
                TableSchema schema = catalog.getTable(tableName);
                if (schema == null) {
                    throw new IllegalStateException("Table does not exist: " + tableName);
                }
                HeapFile heapFile = storageEngine.getHeapFile(tableName);
                TableSchema aliasedSchema = new TableSchema(aliasName, schema.getColumns());
                List<Tuple> tuples = heapFile.scanAll();
                List<Tuple> aliasedTuples = new ArrayList<>();
                for (Tuple t : tuples) {
                    aliasedTuples.add(new Tuple(aliasedSchema, t.getValues()));
                }
                return new ResolvedRelation(aliasedTuples, aliasedSchema);
            }
        }

        // Bare subquery in FROM (no alias)
        if (from instanceof SqlSelect subSelect) {
            List<Tuple> subResults = executeSubSelect(subSelect);
            TableSchema subSchema = !subResults.isEmpty()
                    ? subResults.get(0).getSchema()
                    : resolveFrom(subSelect.getFrom()).schema;
            return new ResolvedRelation(subResults, subSchema);
        }

        throw new UnsupportedOperationException("Unsupported FROM clause: " + from.getClass().getSimpleName());
    }

    private ResolvedRelation resolveJoin(SqlJoin join) {
        ResolvedRelation left = resolveFrom(join.getLeft());
        ResolvedRelation right = resolveFrom(join.getRight());

        String leftAlias = left.schema.getTableName();
        String rightAlias = right.schema.getTableName();
        TableSchema mergedSchema = TableSchema.merge(leftAlias, left.schema, rightAlias, right.schema);

        SqlNode condition = join.getCondition();
        Predicate<Tuple> onPredicate = predicateBuilder.build(condition, mergedSchema, this::executeSubSelect);

        JoinType joinType = join.getJoinType();
        List<Tuple> results = new ArrayList<>();

        if (joinType == JoinType.INNER || joinType == JoinType.COMMA) {
            for (Tuple l : left.tuples) {
                for (Tuple r : right.tuples) {
                    Tuple merged = Tuple.merge(l, r, mergedSchema);
                    if (onPredicate.test(merged)) {
                        results.add(merged);
                    }
                }
            }
        } else if (joinType == JoinType.LEFT) {
            int rightColCount = right.schema.getColumnCount();
            for (Tuple l : left.tuples) {
                boolean matched = false;
                for (Tuple r : right.tuples) {
                    Tuple merged = Tuple.merge(l, r, mergedSchema);
                    if (onPredicate.test(merged)) {
                        results.add(merged);
                        matched = true;
                    }
                }
                if (!matched) {
                    // Add left row with NULLs for right columns
                    Object[] nullRight = new Object[rightColCount];
                    Tuple nullRightTuple = new Tuple(right.schema, nullRight);
                    results.add(Tuple.merge(l, nullRightTuple, mergedSchema));
                }
            }
        } else {
            throw new UnsupportedOperationException("Unsupported join type: " + joinType);
        }

        return new ResolvedRelation(results, mergedSchema);
    }

    private boolean isSelectStar(SqlNodeList selectList) {
        return selectList.size() == 1 && selectList.get(0).toString().equals("*");
    }

    private ProjectionResult applyProjection(SqlNodeList selectList, List<Tuple> tuples, TableSchema schema) {
        int[] indices = new int[selectList.size()];
        List<ColumnDefinition> projectedCols = new ArrayList<>();

        for (int i = 0; i < selectList.size(); i++) {
            SqlNode colNode = selectList.get(i);
            if (!(colNode instanceof SqlIdentifier colId)) {
                throw new UnsupportedOperationException("Only column names are supported in SELECT list, got: " + colNode);
            }
            String colName;
            if (colId.names.size() == 2) {
                colName = colId.names.get(0) + "." + colId.names.get(1);
            } else {
                colName = colId.getSimple();
            }
            int idx = schema.getColumnIndex(colName);
            if (idx < 0) {
                throw new IllegalArgumentException("Unknown column: " + colName);
            }
            indices[i] = idx;
            projectedCols.add(schema.getColumn(idx));
        }

        TableSchema projectedSchema = new TableSchema(schema.getTableName(), projectedCols);
        List<Tuple> projectedTuples = new ArrayList<>();
        for (Tuple t : tuples) {
            Object[] values = new Object[indices.length];
            for (int i = 0; i < indices.length; i++) {
                values[i] = t.getValue(indices[i]);
            }
            projectedTuples.add(new Tuple(projectedSchema, values));
        }

        return new ProjectionResult(projectedTuples, projectedSchema);
    }

    private record ResolvedRelation(List<Tuple> tuples, TableSchema schema) {}
    private record ProjectionResult(List<Tuple> tuples, TableSchema schema) {}
}
