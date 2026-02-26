package com.mpdb.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SlottedPage {

    public static final int PAGE_SIZE = 4096;
    private static final int HEADER_SIZE = 8; // 2B slotCount + 2B freeSpacePtr + 4B pageId
    static final int SLOT_SIZE = 4;   // 2B offset + 2B length
    private static final short DELETED_SENTINEL = (short) 0xFFFF;

    private final byte[] data;
    private final int pageId;

    public SlottedPage(int pageId) {
        this.data = new byte[PAGE_SIZE];
        this.pageId = pageId;
        ByteBuffer buf = ByteBuffer.wrap(data);
        buf.putShort((short) 0);           // slotCount
        buf.putShort((short) PAGE_SIZE);    // freeSpacePtr (points to end)
        buf.putInt(pageId);                 // pageId
    }

    /** Reconstruct a page from raw bytes loaded from disk. */
    public SlottedPage(byte[] rawData) {
        if (rawData.length != PAGE_SIZE) {
            throw new IllegalArgumentException("Page data must be " + PAGE_SIZE + " bytes");
        }
        this.data = rawData;
        this.pageId = ByteBuffer.wrap(data, 4, 4).getInt();
    }

    /** Returns the backing byte array for writing to disk. */
    public byte[] getRawData() {
        return data;
    }

    public int getPageId() {
        return pageId;
    }

    public int getSlotCount() {
        return Short.toUnsignedInt(ByteBuffer.wrap(data, 0, 2).getShort());
    }

    private int getFreeSpacePtr() {
        return Short.toUnsignedInt(ByteBuffer.wrap(data, 2, 2).getShort());
    }

    private void setSlotCount(int count) {
        ByteBuffer.wrap(data, 0, 2).putShort((short) count);
    }

    private void setFreeSpacePtr(int ptr) {
        ByteBuffer.wrap(data, 2, 2).putShort((short) ptr);
    }

    public int getFreeSpace() {
        int slotDirectoryEnd = HEADER_SIZE + getSlotCount() * SLOT_SIZE;
        return getFreeSpacePtr() - slotDirectoryEnd;
    }

    public int insertTuple(byte[] tupleData) {
        int needed = tupleData.length + SLOT_SIZE;
        if (getFreeSpace() < needed) {
            return -1; // no room
        }

        int newFreeSpacePtr = getFreeSpacePtr() - tupleData.length;
        System.arraycopy(tupleData, 0, data, newFreeSpacePtr, tupleData.length);

        int slotIndex = getSlotCount();
        int slotOffset = HEADER_SIZE + slotIndex * SLOT_SIZE;
        ByteBuffer.wrap(data, slotOffset, SLOT_SIZE)
                .putShort((short) newFreeSpacePtr)
                .putShort((short) tupleData.length);

        setSlotCount(slotIndex + 1);
        setFreeSpacePtr(newFreeSpacePtr);

        return slotIndex;
    }

    public byte[] getTuple(int slotIndex) {
        if (slotIndex < 0 || slotIndex >= getSlotCount()) {
            return null;
        }
        int slotOffset = HEADER_SIZE + slotIndex * SLOT_SIZE;
        ByteBuffer slotBuf = ByteBuffer.wrap(data, slotOffset, SLOT_SIZE);
        short offset = slotBuf.getShort();
        short length = slotBuf.getShort();

        if (offset == DELETED_SENTINEL) {
            return null; // deleted
        }

        int off = Short.toUnsignedInt(offset);
        int len = Short.toUnsignedInt(length);
        byte[] tupleData = new byte[len];
        System.arraycopy(data, off, tupleData, 0, len);
        return tupleData;
    }

    public boolean deleteTuple(int slotIndex) {
        if (slotIndex < 0 || slotIndex >= getSlotCount()) {
            return false;
        }
        int slotOffset = HEADER_SIZE + slotIndex * SLOT_SIZE;
        short currentOffset = ByteBuffer.wrap(data, slotOffset, 2).getShort();
        if (currentOffset == DELETED_SENTINEL) {
            return false; // already deleted
        }
        ByteBuffer.wrap(data, slotOffset, 2).putShort(DELETED_SENTINEL);
        return true;
    }

    public List<Integer> getActiveSlots() {
        List<Integer> active = new ArrayList<>();
        int count = getSlotCount();
        for (int i = 0; i < count; i++) {
            int slotOffset = HEADER_SIZE + i * SLOT_SIZE;
            short offset = ByteBuffer.wrap(data, slotOffset, 2).getShort();
            if (offset != DELETED_SENTINEL) {
                active.add(i);
            }
        }
        return active;
    }
}
