package com.mpdb.storage;

import com.mpdb.catalog.ColumnDefinition;
import com.mpdb.catalog.TableSchema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class TupleSerializer {

    public byte[] serialize(Tuple tuple) {
        TableSchema schema = tuple.getSchema();
        int totalSize = calculateSize(tuple);
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(totalSize);

        for (int i = 0; i < schema.getColumnCount(); i++) {
            ColumnDefinition col = schema.getColumn(i);
            Object value = tuple.getValue(i);

            switch (col.type()) {
                case INT -> buffer.putInt((Integer) value);
                case BOOLEAN -> buffer.put((byte) ((Boolean) value ? 1 : 0));
                case VARCHAR -> {
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

        Object[] values = new Object[schema.getColumnCount()];
        for (int i = 0; i < schema.getColumnCount(); i++) {
            ColumnDefinition col = schema.getColumn(i);

            switch (col.type()) {
                case INT -> values[i] = buffer.getInt();
                case BOOLEAN -> values[i] = buffer.get() != 0;
                case VARCHAR -> {
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
        int size = 4; // total size header
        for (int i = 0; i < schema.getColumnCount(); i++) {
            ColumnDefinition col = schema.getColumn(i);
            switch (col.type()) {
                case INT -> size += 4;
                case BOOLEAN -> size += 1;
                case VARCHAR -> {
                    byte[] bytes = ((String) tuple.getValue(i)).getBytes(StandardCharsets.UTF_8);
                    size += 4 + bytes.length;
                }
            }
        }
        return size;
    }
}
