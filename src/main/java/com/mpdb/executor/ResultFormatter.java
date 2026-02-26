package com.mpdb.executor;

import com.mpdb.catalog.ColumnDefinition;
import com.mpdb.catalog.TableSchema;
import com.mpdb.storage.Tuple;

import java.util.List;

public class ResultFormatter {

    public static String format(List<Tuple> tuples, TableSchema schema) {
        if (tuples.isEmpty()) {
            return "(0 rows)";
        }

        int colCount = schema.getColumnCount();
        String[] headers = new String[colCount];
        int[] widths = new int[colCount];

        for (int i = 0; i < colCount; i++) {
            headers[i] = schema.getColumn(i).name();
            widths[i] = headers[i].length();
        }

        // Calculate column widths
        String[][] cells = new String[tuples.size()][colCount];
        for (int r = 0; r < tuples.size(); r++) {
            Tuple t = tuples.get(r);
            for (int c = 0; c < colCount; c++) {
                Object val = t.getValue(c);
                cells[r][c] = val == null ? "NULL" : val.toString();
                widths[c] = Math.max(widths[c], cells[r][c].length());
            }
        }

        StringBuilder sb = new StringBuilder();

        // Header row
        appendRow(sb, headers, widths);
        // Separator
        appendSeparator(sb, widths);
        // Data rows
        for (String[] row : cells) {
            appendRow(sb, row, widths);
        }
        sb.append("(").append(tuples.size()).append(tuples.size() == 1 ? " row)" : " rows)");

        return sb.toString();
    }

    private static void appendRow(StringBuilder sb, String[] values, int[] widths) {
        sb.append(" ");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(" | ");
            sb.append(String.format("%-" + widths[i] + "s", values[i]));
        }
        sb.append("\n");
    }

    private static void appendSeparator(StringBuilder sb, int[] widths) {
        sb.append("-");
        for (int i = 0; i < widths.length; i++) {
            if (i > 0) sb.append("-+-");
            sb.append("-".repeat(widths[i]));
        }
        sb.append("\n");
    }
}
