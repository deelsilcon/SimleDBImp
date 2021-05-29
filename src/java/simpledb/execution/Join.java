package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child1;
    private OpIterator child2;
    private final JoinPredicate jp;
    //A global tuple to store the current state of child1
    private static Tuple t1 = null;


    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.child1 = child1;
        this.child2 = child2;
        this.jp = p;
    }

    public JoinPredicate getJoinPredicate() {
        return jp;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     * alias or table name.
     */
    public String getJoinField1Name() {
        return child1.getTupleDesc().getFieldName(jp.getField1());
    }

    /**
     * @return the field name of join field2. Should be quantified by
     * alias or table name.
     */
    public String getJoinField2Name() {
        return child2.getTupleDesc().getFieldName(jp.getField2());
    }

    /**
     * @see simpledb.storage.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     * implementation logic.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child1.open();
        child2.open();
        super.open();
    }

    @Override
    public void close() {
        super.close();
        child2.close();
        child1.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while (this.child1.hasNext() || t1 != null) {
            if (this.child1.hasNext() && t1 == null) {
                t1 = child1.next();
            }
            while (child2.hasNext()) {
                Tuple t2 = child2.next();
                if (jp.filter(t1, t2)) {
                    TupleDesc td1 = t1.getTupleDesc();
                    TupleDesc td2 = t2.getTupleDesc();
                    Tuple newTuple = new Tuple(TupleDesc.merge(td1, td2));
                    newTuple.setRecordId(t1.getRecordId());
                    int i;
                    for (i = 0; i < td1.numFields(); ++i) {
                        newTuple.setField(i, t1.getField(i));
                    }
                    for (int j = 0; j < td2.numFields(); ++j) {
                        newTuple.setField(i + j, t2.getField(j));
                    }
                    if (!child2.hasNext()) {
                        child2.rewind();
                        t1 = null;
                    }
                    return newTuple;
                }
            }
            child2.rewind();
            t1 = null;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child1, child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child1 = children[0];
        child2 = children[1];
    }

}
