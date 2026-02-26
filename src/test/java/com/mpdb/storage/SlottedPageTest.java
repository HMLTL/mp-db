package com.mpdb.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SlottedPageTest {

    private SlottedPage page;

    @BeforeEach
    void setUp() {
        page = new SlottedPage(0);
    }

    @Test
    void newPage_shouldHaveNoSlots() {
        assertEquals(0, page.getSlotCount());
    }

    @Test
    void newPage_shouldHaveFullFreeSpace() {
        // PAGE_SIZE(4096) - HEADER(8) = 4088
        assertEquals(4088, page.getFreeSpace());
    }

    @Test
    void insertTuple_shouldReturnSlotIndex() {
        byte[] data = new byte[100];
        int slot = page.insertTuple(data);
        assertEquals(0, slot);
        assertEquals(1, page.getSlotCount());
    }

    @Test
    void insertAndRetrieve_shouldReturnSameData() {
        byte[] data = {1, 2, 3, 4, 5};
        int slot = page.insertTuple(data);

        byte[] retrieved = page.getTuple(slot);
        assertArrayEquals(data, retrieved);
    }

    @Test
    void multipleInserts_shouldReturnIncrementingSlots() {
        byte[] data = new byte[10];
        assertEquals(0, page.insertTuple(data));
        assertEquals(1, page.insertTuple(data));
        assertEquals(2, page.insertTuple(data));
        assertEquals(3, page.getSlotCount());
    }

    @Test
    void deleteTuple_shouldMakeTupleUnretrievable() {
        byte[] data = {1, 2, 3};
        int slot = page.insertTuple(data);

        assertTrue(page.deleteTuple(slot));
        assertNull(page.getTuple(slot));
    }

    @Test
    void deleteTuple_alreadyDeleted_shouldReturnFalse() {
        byte[] data = {1, 2, 3};
        int slot = page.insertTuple(data);

        assertTrue(page.deleteTuple(slot));
        assertFalse(page.deleteTuple(slot));
    }

    @Test
    void deleteTuple_invalidSlot_shouldReturnFalse() {
        assertFalse(page.deleteTuple(0));
        assertFalse(page.deleteTuple(-1));
        assertFalse(page.deleteTuple(100));
    }

    @Test
    void getActiveSlots_shouldExcludeDeleted() {
        byte[] data = new byte[10];
        page.insertTuple(data); // slot 0
        page.insertTuple(data); // slot 1
        page.insertTuple(data); // slot 2

        page.deleteTuple(1);

        List<Integer> active = page.getActiveSlots();
        assertEquals(List.of(0, 2), active);
    }

    @Test
    void fillToCapacity_shouldReturnNegativeWhenFull() {
        // Each insert uses data + 4 bytes for slot directory entry
        byte[] data = new byte[100];
        int insertCount = 0;
        while (true) {
            int slot = page.insertTuple(data);
            if (slot < 0) break;
            insertCount++;
        }
        assertTrue(insertCount > 0);
        // Verify subsequent inserts fail
        assertEquals(-1, page.insertTuple(data));
    }

    @Test
    void freeSpace_shouldDecreaseAfterInsert() {
        int initialFreeSpace = page.getFreeSpace();
        byte[] data = new byte[50];
        page.insertTuple(data);

        // Free space decreases by data size + slot directory entry (4 bytes)
        assertEquals(initialFreeSpace - 50 - 4, page.getFreeSpace());
    }

    @Test
    void getTuple_invalidSlot_shouldReturnNull() {
        assertNull(page.getTuple(0));
        assertNull(page.getTuple(-1));
        assertNull(page.getTuple(100));
    }

    @Test
    void getPageId_shouldReturnCorrectId() {
        SlottedPage p = new SlottedPage(42);
        assertEquals(42, p.getPageId());
    }
}
