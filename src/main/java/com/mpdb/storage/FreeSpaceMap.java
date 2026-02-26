package com.mpdb.storage;

import java.util.ArrayList;
import java.util.List;

/**
 * Tracks free space per page to avoid O(n) scans on insert.
 * Each entry stores the free bytes available in the corresponding page.
 */
public class FreeSpaceMap {

    private final List<Integer> freeBytes = new ArrayList<>();

    public void addPage(int pageIndex, int freeSpace) {
        while (freeBytes.size() <= pageIndex) {
            freeBytes.add(0);
        }
        freeBytes.set(pageIndex, freeSpace);
    }

    public void updatePage(int pageIndex, int freeSpace) {
        if (pageIndex < freeBytes.size()) {
            freeBytes.set(pageIndex, freeSpace);
        }
    }

    /**
     * Returns the index of the first page with at least {@code needed} bytes free,
     * or -1 if no page has enough space.
     */
    public int findPageWithSpace(int needed) {
        for (int i = 0; i < freeBytes.size(); i++) {
            if (freeBytes.get(i) >= needed) {
                return i;
            }
        }
        return -1;
    }

    public int getPageCount() {
        return freeBytes.size();
    }

    public int getFreeSpace(int pageIndex) {
        if (pageIndex < 0 || pageIndex >= freeBytes.size()) {
            return 0;
        }
        return freeBytes.get(pageIndex);
    }
}
