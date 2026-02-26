package com.mpdb.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Manages reading and writing fixed-size pages to a file on disk.
 * File layout: consecutive 4096-byte pages, no header.
 */
public class DiskPageManager implements AutoCloseable {

    private final RandomAccessFile file;
    private final Path filePath;

    public DiskPageManager(Path filePath) throws IOException {
        Files.createDirectories(filePath.getParent());
        this.filePath = filePath;
        this.file = new RandomAccessFile(filePath.toFile(), "rw");
    }

    public int getPageCount() throws IOException {
        long length = file.length();
        return (int) (length / SlottedPage.PAGE_SIZE);
    }

    public byte[] readPage(int pageIndex) throws IOException {
        byte[] data = new byte[SlottedPage.PAGE_SIZE];
        long offset = (long) pageIndex * SlottedPage.PAGE_SIZE;
        file.seek(offset);
        file.readFully(data);
        return data;
    }

    public void writePage(int pageIndex, byte[] data) throws IOException {
        long offset = (long) pageIndex * SlottedPage.PAGE_SIZE;
        file.seek(offset);
        file.write(data, 0, SlottedPage.PAGE_SIZE);
        file.getFD().sync();
    }

    public void sync() throws IOException {
        file.getFD().sync();
    }

    @Override
    public void close() throws IOException {
        file.close();
    }

    public void delete() throws IOException {
        file.close();
        Files.deleteIfExists(filePath);
    }
}
