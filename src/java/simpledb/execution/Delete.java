package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;


import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator child;
    private TupleDesc td;
    private int cnt;
    private boolean called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.tid = t;
        this.child = child;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Number of deleted tuples"});
        this.cnt = -1;
        this.called = false;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
        cnt = 0;
    }

    @Override
    public void close() {
        super.close();
        child.close();
        cnt = -1;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        cnt = 0;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(called){
            return null;
        }
        called = true;
        while (child.hasNext()) {
            Tuple t = child.next();
            Database.getBufferPool().deleteTuple(tid, t);
            cnt++;
        }
        Tuple t = new Tuple(td);
        t.setField(0, new IntField(cnt));
        return t;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }

}
