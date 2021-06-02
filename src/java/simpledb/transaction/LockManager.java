package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author deelsilcon
 */
public class LockManager {
    ConcurrentMap<TransactionId, List<ReadWriteLock>> transactionLockMap;
    ConcurrentMap<PageId, ReadWriteLock> lockPageMap;
    DependencyGraph dg = new DependencyGraph();
    private final byte[] aLock = new byte[0];

    public LockManager() {
        transactionLockMap = new ConcurrentHashMap<>();
        lockPageMap = new ConcurrentHashMap<>();
    }

    public void acquireLock(PageId pid, TransactionId tid, LockType type) {
        synchronized (aLock) {
            if (lockPageMap.get(pid) == null) {
                // if no transaction hold this lock, just create a new lock
                ReadWriteLock lock = new ReadWriteLock(true, pid);
                if (type == LockType.ReadLock) {
                    lock.readLock();
                    lock.addRTids(tid);
                } else {
                    lock.writeLock();
                    lock.setwTid(tid);
                }
                if (transactionLockMap.get(tid) == null) {
                    List<ReadWriteLock> locks = new ArrayList<>();
                    locks.add(lock);
                    transactionLockMap.put(tid, locks);
                } else {
                    transactionLockMap.get(tid).add(lock);
                }
                lockPageMap.put(pid, lock);
                return;
            }

            List<ReadWriteLock> locks = transactionLockMap.get(tid);
            if (locks != null) {
                for (ReadWriteLock lock : locks) {
                    if (lock.getPid().equals(pid)) {
                        if (lock.isLockHoldByOneReader() && type == LockType.WriteLock) {
                            lock.updateLock();
                            lock.setwTid(tid);
                            lock.removeRtid(tid);
                            return;
                        } else {
                            //just do a little tricks here
                            return;
                        }
                    }
                }
            } else {
                locks = new ArrayList<>();
            }

            ReadWriteLock lock = lockPageMap.get(pid);
            if (type == LockType.ReadLock) {
                lock.readLock();
                lock.addRTids(tid);
            } else {
                lock.writeLock();
                lock.setwTid(tid);
            }
            locks.add(lock);
            transactionLockMap.put(tid, locks);
        }
    }

    public synchronized void releaseLock(PageId pid, TransactionId tid) {
        // if not a single lock is held on pid
        assert lockPageMap.get(pid) != null : "page not locked!";
        List<ReadWriteLock> locks = transactionLockMap.get(tid);
        int sz = locks.size();
        for (int i = sz - 1; i >= 0; --i) {
            ReadWriteLock lock = locks.get(i);
            if (lock.getPid().equals(pid)) {
                if (lock.getLockType() == LockType.ReadLock) {
                    lock.readUnlock();
                    lock.removeRtid(tid);
                } else {
                    lock.writeUnlock();
                    lock.setwTid(null);
                }
                locks.remove(lock);
                if (locks.size() == 0) {
                    transactionLockMap.remove(tid);
                } else {
                    transactionLockMap.put(tid, locks);
                }
            }
        }
    }


    public synchronized boolean holdsLock(PageId pid, TransactionId tid) {
        List<ReadWriteLock> locks = transactionLockMap.get(tid);
        if (locks == null) {
            return false;
        }
        for (ReadWriteLock lock : locks) {
            if (lock.getPid().equals(pid)) {
                return true;
            }
        }
        return false;
    }

    public void resetDg() {
        dg.reset();
    }

    public List<ReadWriteLock> getTransactionLocks(TransactionId tid) {
        return transactionLockMap.get(tid);
    }

    //    helper methods

    public synchronized boolean processConflict(PageId pid, TransactionId tid, LockType type) {
        if (lockPageMap.get(pid) == null) {
            return false;
        }
        ReadWriteLock lock = lockPageMap.get(pid);
        if (tid.equals(lock.getwTid()) || (lock.isLockHoldByOneReader() && lock.getRTids().get(0).equals(tid))) {
            return false;
        }

        if (lock.getLockType() == LockType.WriteLock) {
            TransactionId tidInLock = lock.getwTid();
            dg.addEdge(tidInLock.getId(), tid.getId());
        } else if (type == LockType.WriteLock) {
            List<TransactionId> ids = lock.getRTids();
            for (TransactionId id : ids) {
                synchronized (this) {
                    dg.addEdge(id.getId(), tid.getId());
                }
            }
        }
        return dg.checkCycle();
//        }
    }

//    public synchronized boolean check() {
//        return dg.checkCycle();
//    }
}