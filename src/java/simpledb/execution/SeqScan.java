package simpledb.execution;

import simpledb.common.Database;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private int tableId;
    private String tableAlias;
    private DbFileIterator it;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableId = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        it = Database.getCatalog().getDatabaseFile(tableId).iterator(tid);
        if (it != null) {
            it.open();
        }
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    @Override
    public TupleDesc getTupleDesc() {
        TupleDesc td = Database.getCatalog().getTupleDesc(tableId);
        int n = td.numFields();
        String[] names = new String[n];
        for (int i = 0; i < n; ++i) {
            names[i] = tableAlias + "." + td.getFieldName(i);
        }
        return new TupleDesc(td.getTypes(), names);
    }

    @Override
    public boolean hasNext() throws TransactionAbortedException, DbException {
        if (it == null) {
            return false;
        }
        return it.hasNext();
    }

    @Override
    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (it == null) {
            throw new NoSuchElementException("Element doesn't exsit");
        }
        return it.next();
    }

    @Override
    public void close() {
        it.close();
    }

    @Override
    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        it.rewind();
    }
}
