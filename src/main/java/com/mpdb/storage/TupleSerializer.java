package com.mpdb.storage;

import com.mpdb.catalog.ColumnDefinition;
import com.mpdb.catalog.TableSchema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class TupleSerializer {

    public byte[] serialize(Tuple tuple) {
        TableSchema schema = tuple.getSchema();
        int colCount = schema.getColumnCount();
        int nullBitmapBytes = (colCount + 7) / 8;
        int totalSize = calculateSize(tuple);
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(totalSize);

        // Write null bitmap
        byte[] nullBitmap = new byte[nullBitmapBytes];
        for (int i = 0; i < colCount; i++) {
            if (tuple.getValue(i) == null) {
                nullBitmap[i / 8] |= (1 << (i % 8));
            }
        }
        buffer.put(nullBitmap);

        // Write column values (skip nulls)
        for (int i = 0; i < colCount; i++) {
            if (tuple.getValue(i) == null) continue;
            ColumnDefinition col = schema.getColumn(i);
            Object value = tuple.getValue(i);

            switch (col.type()) {
                case INT -> buffer.putInt((Integer) value);
                case FLOAT -> buffer.putFloat((Float) value);
                case BOOLEAN -> buffer.put((byte) ((Boolean) value ? 1 : 0));
                case VARCHAR, TEXT -> {
                    byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                    buffer.putInt(bytes.length);
                    buffer.put(bytes);
                }
            }
        }
        return buffer.array();
    }

    public Tuple deserialize(byte[] data, TableSchema schema) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int totalSize = buffer.getInt(); // read past size header
        int colCount = schema.getColumnCount();
        int nullBitmapBytes = (colCount + 7) / 8;

        // Read null bitmap
        byte[] nullBitmap = new byte[nullBitmapBytes];
        buffer.get(nullBitmap);

        Object[] values = new Object[colCount];
        for (int i = 0; i < colCount; i++) {
            boolean isNull = (nullBitmap[i / 8] & (1 << (i % 8))) != 0;
            if (isNull) {
                values[i] = null;
                continue;
            }
            ColumnDefinition col = schema.getColumn(i);

            switch (col.type()) {
                case INT -> values[i] = buffer.getInt();
                case FLOAT -> values[i] = buffer.getFloat();
                case BOOLEAN -> values[i] = buffer.get() != 0;
                case VARCHAR, TEXT -> {
                    int len = buffer.getInt();
                    byte[] bytes = new byte[len];
                    buffer.get(bytes);
                    values[i] = new String(bytes, StandardCharsets.UTF_8);
                }
            }
        }
        return new Tuple(schema, values);
    }

    private int calculateSize(Tuple tuple) {
        TableSchema schema = tuple.getSchema();
        int colCount = schema.getColumnCount();
        int size = 4; // total size header
        size += (colCount + 7) / 8; // null bitmap
        for (int i = 0; i < colCount; i++) {
            if (tuple.getValue(i) == null) continue;
            ColumnDefinition col = schema.getColumn(i);
            switch (col.type()) {
                case INT -> size += 4;
                case FLOAT -> size += 4;
                case BOOLEAN -> size += 1;
                case VARCHAR, TEXT -> {
                    byte[] bytes = ((String) tuple.getValue(i)).getBytes(StandardCharsets.UTF_8);
                    size += 4 + bytes.length;
                }
            }
        }
        return size;
    }
}
