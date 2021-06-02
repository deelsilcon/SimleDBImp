package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.storage.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    private File file;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) {
        Page page = null;
        byte[] data = new byte[BufferPool.getPageSize()];

        try (RandomAccessFile rf = new RandomAccessFile(getFile(), "r")) {
            int pos = pid.getPageNumber() * BufferPool.getPageSize();
            rf.seek(pos);
            rf.read(data, 0, data.length);
            page = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    @Override
    public void writePage(Page page) throws IOException {
        byte[] data = page.getPageData();
        try (RandomAccessFile rf = new RandomAccessFile(getFile(), "rw")) {
            int pos = page.getId().getPageNumber() * BufferPool.getPageSize();
            rf.seek(pos);
            rf.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.floorDiv(file.length(), BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    @Override
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> pages = new ArrayList<>();
        //1. Try to find empty slot from existed page

        for (int i = 0; i < numPages(); ++i) {
            HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if (hp.getNumEmptySlots() != 0) {
                hp.insertTuple(t);
                pages.add(hp);
                return pages;
            }
        }
        //2. Create a new page
        synchronized (this){
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file, true));
            byte[] data = HeapPage.createEmptyPageData();
            bos.write(data);
            bos.close();

            HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), numPages() - 1), Permissions.READ_WRITE);
            hp.insertTuple(t);
            pages.add(hp);
        }
        return pages;
    }

    // see DbFile.java for javadocs
    @Override
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        hp.deleteTuple(t);
        List<Page> ls = new ArrayList<>();
        ls.add(hp);
        return ls;
    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

    private static final class HeapFileIterator implements DbFileIterator {
        private final HeapFile heapFile;
        private final TransactionId tid;
        private Iterator<Tuple> it;
        private int cur;


        public HeapFileIterator(HeapFile file, TransactionId tid) {
            this.heapFile = file;
            this.tid = tid;
        }

        private Iterator<Tuple> getTuplesInPage(int pageNum)
                throws TransactionAbortedException, DbException {
            if (pageNum >= 0 && pageNum < heapFile.numPages()) {
                HeapPageId hid = new HeapPageId(heapFile.getId(), pageNum);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, hid, Permissions.READ_ONLY);
                return page.iterator();
            }
            throw new DbException("Oops!");
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            cur = 0;
            it = getTuplesInPage(cur);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (it == null) {
                return false;
            }
            if (it.hasNext()) {
                return true;
            } else {
                while (!it.hasNext()) {
                    if (++cur >= heapFile.numPages()) {
                        return false;
                    } else {
                        it = getTuplesInPage(cur);
                    }
                }
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null) {
                throw new NoSuchElementException("Null pointer");
            }
            if (it.hasNext()) {
                return it.next();
            }
            throw new NoSuchElementException("No such element");
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            it = null;
        }
    }
}



