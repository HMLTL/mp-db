package com.mpdb.catalog;

public enum ColumnType {
    INT(4, true),
    FLOAT(4, true),
    VARCHAR(0, false),
    TEXT(0, false),
    BOOLEAN(1, true);

    private final int fixedSize;
    private final boolean fixedLength;

    ColumnType(int fixedSize, boolean fixedLength) {
        this.fixedSize = fixedSize;
        this.fixedLength = fixedLength;
    }

    public int getFixedSize() {
        return fixedSize;
    }

    public boolean isFixedLength() {
        return fixedLength;
    }
}
