package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.*;

import java.io.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final int numOfPages;

    private final ConcurrentMap<PageId, Page> page_map;
    //    private int age;
    private final LockManager lockManager;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numOfPages = numPages;
//        page_map = Collections.synchronizedMap((new LinkedHashMap<>(numPages, 0.75f, true)));
        this.page_map = new ConcurrentHashMap<>();
//        this.age = 0;
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        LockType type = (perm == Permissions.READ_WRITE ? LockType.WriteLock : LockType.ReadLock);
        if (lockManager.processConflict(pid, tid, type)) {
            lockManager.resetDg();
            throw new TransactionAbortedException();
        }
        lockManager.acquireLock(pid, tid, type);
        if (!page_map.containsKey(pid)) {
            if (page_map.size() > numOfPages) {
                evictPage();
            }
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            page_map.put(pid, page);
        }
        return page_map.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        List<ReadWriteLock> locks = lockManager.getTransactionLocks(tid);
        if (locks == null) {
            return;
        }
        int sz = locks.size();
        for (int i = sz - 1; i >= 0; --i) {
            ReadWriteLock lock = locks.get(i);
            lockManager.releaseLock(lock.getPid(), tid);
        }
        if (commit) {
            flushPages(tid);
        } else {
            restorePages(tid);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, TransactionAbortedException, IOException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        updatePage(file.insertTuple(tid, t), tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = null;
        try {
            pages = file.deleteTuple(tid, t);
            for (Page p : pages) {
                p.markDirty(true, tid);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() {
        for (Page page : page_map.values()) {
            try {
                flushPage(page.getId());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     */
    public synchronized void discardPage(PageId pid) {
        page_map.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = page_map.get(pid);
        if (page.isDirty() != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            //mark that page as not dirty
            page.markDirty(false, null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) {
        for (PageId pid : page_map.keySet()) {
            Page page = page_map.get(pid);
            if (page.isDirty() == tid) {
                try {
                    flushPage(pid);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        Page pageToEvict = null;
        for (PageId pid : page_map.keySet()) {
            Page page = page_map.get(pid);
            if (page.isDirty() == null) {
                pageToEvict = page;
                break;
            }
        }
        try {
            assert pageToEvict != null;
            flushPage(pageToEvict.getId());
        } catch (IOException e) {
            e.printStackTrace();
        }
        discardPage(pageToEvict.getId());
    }

    private void updatePage(List<Page> pages, TransactionId tid) throws DbException {
        for (Page p : pages) {
            p.markDirty(true, tid);
            //Since we update the page, we may need to evict a page from buffer pool if necessary
            if (page_map.size() > numOfPages) {
                evictPage();
            }
            page_map.put(p.getId(), p);
        }
    }

    private synchronized void restorePages(TransactionId tid) {
        for (PageId pid : page_map.keySet()) {
            Page page = page_map.get(pid);

            //dirty because of this aborted transaction, we need to read those pages back from disk
            if (page.isDirty() == tid) {
                int tableId = pid.getTableId();
                DbFile file = Database.getCatalog().getDatabaseFile(tableId);
                Page originPage = file.readPage(pid);
                page_map.put(pid, originPage);
            }
        }
    }

    private boolean checkNoBufferPoolState(TransactionId tid){
        return lockManager.getTransactionLocks(tid) == null;
    }

}
