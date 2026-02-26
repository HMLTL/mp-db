package com.mpdb.storage;

import com.mpdb.catalog.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class HeapFile {

    private static final Logger log = LoggerFactory.getLogger(HeapFile.class);

    private final List<SlottedPage> pages = new ArrayList<>();
    private final FreeSpaceMap freeSpaceMap = new FreeSpaceMap();
    private final TableSchema schema;
    private final TupleSerializer serializer = new TupleSerializer();
    private final DiskPageManager diskManager; // null for in-memory only

    public HeapFile(TableSchema schema) {
        this.schema = schema;
        this.diskManager = null;
    }

    public HeapFile(TableSchema schema, DiskPageManager diskManager) {
        this.schema = schema;
        this.diskManager = diskManager;
        loadFromDisk();
    }

    private void loadFromDisk() {
        if (diskManager == null) return;
        try {
            int pageCount = diskManager.getPageCount();
            for (int i = 0; i < pageCount; i++) {
                byte[] rawData = diskManager.readPage(i);
                SlottedPage page = new SlottedPage(rawData);
                pages.add(page);
                freeSpaceMap.addPage(i, page.getFreeSpace());
            }
            if (pageCount > 0) {
                log.debug("Loaded {} page(s) for table '{}'", pageCount, schema.getTableName());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load heap file for table: " + schema.getTableName(), e);
        }
    }

    public TupleId insertTuple(Tuple tuple) {
        byte[] data = serializer.serialize(tuple);
        int needed = data.length + SlottedPage.SLOT_SIZE;

        // Use free-space map to find a page with enough room
        int pageIndex = freeSpaceMap.findPageWithSpace(needed);
        if (pageIndex >= 0) {
            SlottedPage page = pages.get(pageIndex);
            int slot = page.insertTuple(data);
            if (slot >= 0) {
                freeSpaceMap.updatePage(pageIndex, page.getFreeSpace());
                flushPage(pageIndex);
                return new TupleId(pageIndex, slot);
            }
        }

        // All pages full — allocate a new one
        SlottedPage newPage = new SlottedPage(pages.size());
        pages.add(newPage);
        int newPageIndex = pages.size() - 1;
        int slot = newPage.insertTuple(data);
        freeSpaceMap.addPage(newPageIndex, newPage.getFreeSpace());
        flushPage(newPageIndex);
        return new TupleId(newPageIndex, slot);
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
        SlottedPage page = pages.get(id.pageIndex());
        boolean deleted = page.deleteTuple(id.slotIndex());
        if (deleted) {
            freeSpaceMap.updatePage(id.pageIndex(), page.getFreeSpace());
            flushPage(id.pageIndex());
        }
        return deleted;
    }

    public int getPageCount() {
        return pages.size();
    }

    public TableSchema getSchema() {
        return schema;
    }

    public void close() {
        if (diskManager != null) {
            try {
                diskManager.close();
            } catch (IOException e) {
                log.warn("Failed to close disk manager for table '{}'", schema.getTableName(), e);
            }
        }
    }

    public void deleteFiles() {
        if (diskManager != null) {
            try {
                diskManager.delete();
            } catch (IOException e) {
                log.warn("Failed to delete data file for table '{}'", schema.getTableName(), e);
            }
        }
    }

    /** Returns the SlottedPage at the given index (for free-space tracking). */
    SlottedPage getPage(int index) {
        return pages.get(index);
    }

    private void flushPage(int pageIndex) {
        if (diskManager == null) return;
        try {
            diskManager.writePage(pageIndex, pages.get(pageIndex).getRawData());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write page " + pageIndex, e);
        }
    }
}
