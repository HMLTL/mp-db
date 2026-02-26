package com.mpdb.catalog;

public record ColumnDefinition(String name, ColumnType type, int maxLength) {

    public ColumnDefinition(String name, ColumnType type) {
        this(name, type, type.getFixedSize());
    }
}
