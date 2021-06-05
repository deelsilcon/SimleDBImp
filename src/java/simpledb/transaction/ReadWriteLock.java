package simpledb.transaction;

import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.ArrayList;
import java.util.List;

/**
 * @author deelsilcon
 */
public class ReadWriteLock {
    private PageId pid;
    private int writingCount;
    private int readingCount;
    private int waitWriter;
    private int waitReader;

    // 偏向读者还是写者的锁
    private boolean preferWriter;


    public ReadWriteLock(boolean preferWriter, PageId pid) {
        this.preferWriter = preferWriter;
        this.pid = pid;
    }

    public synchronized void readLock() {
        this.waitReader++;
        try {
            while (writingCount > 0 || (preferWriter && waitWriter > 0)) {
                this.wait();
            }
            this.readingCount++;
        } catch (InterruptedException ignored) {
        } finally {
            this.waitReader--;
        }
    }

    public synchronized void readUnlock() {
        this.readingCount--;
        this.notifyAll();
    }

    public synchronized void writeLock() {
        this.waitWriter++;
        try {
            while (writingCount > 0 || readingCount > 0) {
                this.wait();
            }
            this.writingCount++;
        } catch (InterruptedException ignored) {
        } finally {
            this.waitWriter--;
        }
    }

    public synchronized void writeUnlock() {
        this.writingCount--;
        this.notifyAll();
    }


    public PageId getPid() {
        return this.pid;
    }

    public LockType getLockType() {
        return this.writingCount > 0 ? LockType.WriteLock : LockType.ReadLock;
    }

    public void updateLock() {
        this.readUnlock();
        this.writeLock();
    }

    public boolean isLockHoldByOneReader() {
        return writingCount == 0 && readingCount == 1;
    }
}
