package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages locks on PageIds held by TransactionIds.
 * S-locks and X-locks are represented as Permissions.READ_ONLY and Permisions.READ_WRITE, respectively
 * <p>
 * All the field read/write operations are protected by this
 */
public class LockManager {

    final int LOCK_WAIT = 10;
    final int LOCK_TIME_OUT = 1000;
    private final ConcurrentMap<TransactionId, Set<PageId>> tidToPidMap;
    private final ConcurrentMap<PageId, Set<TransactionId>> pidToTidMap;
    private final ConcurrentMap<PageId, Permissions> lockTypeMap;
    private final DependencyGraph dg;

    /**
     * Sets up the lock manager to keep track of page-level locks for transactions
     * Should initialize state required for the lock table data structure(s)
     */
    public LockManager() {
        // Given a transactionId, which pages does it have locked?
        // Given a page Id, which transactions hold a lock on the page?
        // Given a page, which Permissions is it locked with?
        this.tidToPidMap = new ConcurrentHashMap<>();
        this.pidToTidMap = new ConcurrentHashMap<>();
        this.lockTypeMap = new ConcurrentHashMap<>();
        this.dg = new DependencyGraph();
    }

    /**
     * Tries to acquire a lock on page pid for transaction tid, with permissions perm.
     * If cannot acquire the lock, waits for a timeout period, then tries again.
     * This method does not return until the lock is granted, or an exception is thrown
     * <p>
     * In Exercise 5, checking for deadlock will be added in this method
     * Note that a transaction should throw a DeadlockException in this method to
     * signal that it should be aborted.
     *
     * @throws DeadlockException after on cycle-based deadlock
     */
    public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm)
            throws DeadlockException {
        boolean ifAdded = addToWaitForGraph(tid, pid, perm);
//        long start = System.currentTimeMillis();
        while (!lock(tid, pid, perm)) {
            // keep trying to get the lock

            synchronized (this) {
//                if(System.currentTimeMillis() - start > LOCK_TIME_OUT){
//                    throw new DeadlockException();
//                }
                if (ifAdded){
                    throw new DeadlockException();
                }
            }

            try {
                // couldn't get lock, wait for some time, then try again
                Thread.sleep(LOCK_WAIT);
            } catch (InterruptedException e) { // do nothing
            }

        }

        synchronized (this) {
            removeFromWaitForGraph(tid, pid, ifAdded);
        }

        return true;
    }

    /**
     * Release all locks corresponding to TransactionId tid.
     * This method is used by BufferPool.transactionComplete()
     */
    public synchronized void releaseAllLocks(TransactionId tid) {
        //to avoid concurrent modification(iterating and deleting), simply copy it first
        List<PageId> pageIdList = new ArrayList<>(getTxnPageIds(tid));
        for (PageId pageId : pageIdList) {
            releaseLock(tid, pageId);
        }
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public synchronized boolean holdsLock(TransactionId tid, PageId p) {
        return getTxnPageIds(tid).contains(p);
    }

    /**
     * Answers the question: is this transaction "locked out" of acquiring lock on this page with this perm?
     * Returns false if this tid/pid/perm lock combo can be achieved (i.e., not locked out), true otherwise.
     * <p>
     * Logic:
     * <p>
     * if perm == READ_ONLY
     * if tid is holding any sort of lock on pid, then the tid can acquire the lock (return false).
     * <p>
     * if another tid is holding a READ lock on pid, then the tid can acquire the lock (return false).
     * if another tid is holding a WRITE lock on pid, then tid can not currently
     * acquire the lock (return true).
     * <p>
     * else
     * if tid is THE ONLY ONE holding a READ lock on pid, then tid can acquire the lock (return false).
     * if tid is holding a WRITE lock on pid, then the tid already has the lock (return false).
     * <p>
     * if another tid is holding any sort of lock on pid, then the tid cannot currenty acquire the lock (return true).
     */
    private synchronized boolean locked(TransactionId tid, PageId pid, Permissions perm) {
        // Just follow the instruction on the documentation
        if (perm == Permissions.READ_ONLY) {
            if (holdsLock(tid, pid)) {
                return false;
            } else {
                Permissions lockType = getLockType(pid);
                return lockType == Permissions.READ_WRITE;
            }
        } else if (holdsLock(tid, pid) && getPageTxns(pid).size() == 1) {
            return false;
        }

        return getPageTxns(pid).size() >= 1;
    }

    /**
     * Releases whatever lock this transaction has on this page
     * Should update lock table data structure(s)
     * <p>
     * Note that you do not need to "wake up" another transaction that is waiting for a lock on this page,
     * since that transaction will be "sleeping" and will wake up and check if the page is available on its own
     * However, if you decide to change the fact that a thread is sleeping in acquireLock(), you would have to wake it up here
     */
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        if (!holdsLock(tid, pid)) {
            System.out.println("[WARNING]: Try release lock on transaction which actually doesn't hold that lock!");
            return;
        }
        if (getPageTxns(pid).size() > 1) {
            setPermission(pid, Permissions.READ_ONLY);
        } else {
            removePermission(pid);
        }
        removePidInTxn(tid, pid);
        removeTxnInPid(pid, tid);
    }



    /**
     * Attempt to lock the given PageId with the given Permissions for this TransactionId
     * Should update the lock table data structure(s) if successful
     * <p>
     * Returns true if the lock attempt was successful, false otherwise
     */
    private synchronized boolean lock(TransactionId tid, PageId pid, Permissions perm) {

        if (locked(tid, pid, perm)) {
            // this transaction cannot get the lock on this page; it is "locked out"
            return false;
        }
        setPermission(pid, perm);
        addPidToTxn(tid, pid);
        addTidToPage(pid, tid);
        return true;
    }

    private synchronized Permissions getLockType(PageId pid) {
        return this.lockTypeMap.getOrDefault(pid, null);
    }

    private synchronized Set<PageId> getTxnPageIds(TransactionId tid) {
        return this.tidToPidMap.getOrDefault(tid, new HashSet<>());
    }

    private synchronized Set<TransactionId> getPageTxns(PageId pid) {
        return this.pidToTidMap.getOrDefault(pid, new HashSet<>());
    }

    private synchronized void setPermission(PageId pid, Permissions perm) {
        if (getLockType(pid) == Permissions.READ_WRITE) {
            return;
        }
        this.lockTypeMap.put(pid, perm);
    }


    private synchronized void addPidToTxn(TransactionId tid, PageId pid) {
        Set<PageId> pageIds = getTxnPageIds(tid);
        pageIds.add(pid);
        this.tidToPidMap.put(tid, pageIds);
    }

    private synchronized void addTidToPage(PageId pid, TransactionId tid) {
        Set<TransactionId> transactionIds = getPageTxns(pid);
        transactionIds.add(tid);
        this.pidToTidMap.put(pid, transactionIds);
    }

    private synchronized void removePermission(PageId pid) {
        if (getLockType(pid) != null) {
            lockTypeMap.remove(pid);
        }
    }

    private synchronized void removePidInTxn(TransactionId tid, PageId pid) {
        getTxnPageIds(tid).remove(pid);
    }

    private synchronized void removeTxnInPid(PageId pid, TransactionId tid) {
        getPageTxns(pid).remove(tid);
    }

    private synchronized  boolean addToWaitForGraph(TransactionId tid, PageId pid, Permissions perm){
        if(locked(tid, pid, perm)){
            for(TransactionId id: getPageTxns(pid)){
                this.dg.addEdge(id.getId(), tid.getId());
            }
            return true;
        }
        return false;
    }

    private synchronized void removeFromWaitForGraph(TransactionId tid, PageId pid, boolean ifAdded){
        if(ifAdded){
            for(TransactionId id: getPageTxns(pid)){
                this.dg.addEdge(id.getId(), tid.getId());
            }
        }
    }

    public boolean hasCycle(){
        return this.dg.checkCycle();
    }
}