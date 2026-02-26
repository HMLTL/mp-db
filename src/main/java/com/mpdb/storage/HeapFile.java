package com.mpdb.storage;

import com.mpdb.catalog.TableSchema;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class HeapFile {

    private final List<SlottedPage> pages = new ArrayList<>();
    private final TableSchema schema;
    private final TupleSerializer serializer = new TupleSerializer();

    public HeapFile(TableSchema schema) {
        this.schema = schema;
    }

    public TupleId insertTuple(Tuple tuple) {
        byte[] data = serializer.serialize(tuple);

        for (int i = 0; i < pages.size(); i++) {
            int slot = pages.get(i).insertTuple(data);
            if (slot >= 0) {
                return new TupleId(i, slot);
            }
        }

        // All pages full — allocate a new one
        SlottedPage newPage = new SlottedPage(pages.size());
        pages.add(newPage);
        int slot = newPage.insertTuple(data);
        return new TupleId(pages.size() - 1, slot);
    }

    public Tuple getTuple(TupleId id) {
        if (id.pageIndex() < 0 || id.pageIndex() >= pages.size()) {
            return null;
        }
        byte[] data = pages.get(id.pageIndex()).getTuple(id.slotIndex());
        if (data == null) {
            return null;
        }
        return serializer.deserialize(data, schema);
    }

    public List<Tuple> scanAll() {
        List<Tuple> results = new ArrayList<>();
        for (SlottedPage page : pages) {
            for (int slot : page.getActiveSlots()) {
                byte[] data = page.getTuple(slot);
                if (data != null) {
                    results.add(serializer.deserialize(data, schema));
                }
            }
        }
        return results;
    }

    public List<Tuple> scanWithFilter(Predicate<Tuple> predicate) {
        List<Tuple> results = new ArrayList<>();
        for (SlottedPage page : pages) {
            for (int slot : page.getActiveSlots()) {
                byte[] data = page.getTuple(slot);
                if (data != null) {
                    Tuple tuple = serializer.deserialize(data, schema);
                    if (predicate.test(tuple)) {
                        results.add(tuple);
                    }
                }
            }
        }
        return results;
    }

    public List<Map.Entry<TupleId, Tuple>> scanAllWithIds() {
        List<Map.Entry<TupleId, Tuple>> results = new ArrayList<>();
        for (int p = 0; p < pages.size(); p++) {
            SlottedPage page = pages.get(p);
            for (int slot : page.getActiveSlots()) {
                byte[] data = page.getTuple(slot);
                if (data != null) {
                    Tuple tuple = serializer.deserialize(data, schema);
                    results.add(new AbstractMap.SimpleEntry<>(new TupleId(p, slot), tuple));
                }
            }
        }
        return results;
    }

    public List<Map.Entry<TupleId, Tuple>> scanWithFilterAndIds(Predicate<Tuple> predicate) {
        List<Map.Entry<TupleId, Tuple>> results = new ArrayList<>();
        for (int p = 0; p < pages.size(); p++) {
            SlottedPage page = pages.get(p);
            for (int slot : page.getActiveSlots()) {
                byte[] data = page.getTuple(slot);
                if (data != null) {
                    Tuple tuple = serializer.deserialize(data, schema);
                    if (predicate.test(tuple)) {
                        results.add(new AbstractMap.SimpleEntry<>(new TupleId(p, slot), tuple));
                    }
                }
            }
        }
        return results;
    }

    public boolean deleteTuple(TupleId id) {
        if (id.pageIndex() < 0 || id.pageIndex() >= pages.size()) {
            return false;
        }
        return pages.get(id.pageIndex()).deleteTuple(id.slotIndex());
    }

    public int getPageCount() {
        return pages.size();
    }

    public TableSchema getSchema() {
        return schema;
    }
}
