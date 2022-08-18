package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        int pageNumber = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        byte[] data = new byte[pageSize];

        RandomAccessFile raFile;
        try {
            raFile = new RandomAccessFile(file.getAbsoluteFile(), "r");
            if ((pageNumber + 1) * pageSize > raFile.length()) {
                throw new IllegalArgumentException();
            }
            int offset = pageNumber * pageSize;
            raFile.seek(offset);
            raFile.read(data, 0, pageSize);
            raFile.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new IllegalArgumentException();
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        int pageNumber = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        byte[] data = page.getPageData();

        RandomAccessFile raFile;
        try {
            raFile = new RandomAccessFile(file.getAbsoluteFile(), "rw");
            int offset = pageNumber * pageSize;
            raFile.seek(offset);
            raFile.write(data, 0, pageSize);
            raFile.close();
        } catch (Exception e) {
            throw new IOException();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pageSize = BufferPool.getPageSize();
        RandomAccessFile raFile;
        int pageNum = 0;
        try {
            raFile = new RandomAccessFile(file.getAbsoluteFile(), "r");
            pageNum = (int) Math.ceil(raFile.length() * 1.0 / pageSize);
            raFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return pageNum;
        }
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        int pageNum = numPages();
        int tableId = getId();
        List<Page> dirtyPages = new ArrayList<>();
        for (int i = 0; i < pageNum; i++) {
            PageId pageId = new HeapPageId(tableId, i);
            Page page = Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            HeapPage heapPage = (HeapPage) page;
            if (heapPage.getNumEmptySlots() > 0) {
                heapPage.insertTuple(t);
                dirtyPages.add(heapPage);
                return dirtyPages;
            }
        }
        byte[] data = HeapPage.createEmptyPageData();
        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(file, true));
        bw.write(data);
        bw.close();
        HeapPage newPage = (HeapPage) Database.getBufferPool().getPage
                (tid, new HeapPageId(tableId, pageNum), Permissions.READ_WRITE);
        newPage.insertTuple(t);
        dirtyPages.add(newPage);
        return dirtyPages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        RecordId recordId = t.getRecordId();
        PageId pageId = recordId.getPageId();
        Page page = Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        if (page == null) {
            throw new DbException("the tuple is not a member of the file.");
        }
        HeapPage heapPage = (HeapPage) page;
        heapPage.deleteTuple(t);
        ArrayList<Page> pages = new ArrayList<>();
        pages.add(page);
        return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int pageCursor = -1;
            private Iterator<Tuple> it;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pageCursor = 0;
                it = null;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (it != null && it.hasNext()) {
                    return true;
                } else if (pageCursor < 0 || pageCursor >= numPages()) {
                    return false;
                } else {
                    HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(
                            tid, new HeapPageId(getId(), pageCursor++), Permissions.READ_ONLY);
                    it = heapPage.iterator();
                    return hasNext();
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                } else {
                    return it.next();
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pageCursor = 0;
                it = null;
            }

            @Override
            public void close() {
                pageCursor = -1;
                it = null;
            }
        };
    }

}

