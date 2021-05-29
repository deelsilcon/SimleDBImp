package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private int gField;
    private Type gbFieldType;
    private int aField;
    private Aggregator.Op aop;
    private Aggregator ag;
    private OpIterator it;
    private OpIterator child;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.aField = afield;
        this.gField = gfield;
        this.aop = aop;
        if (gfield == Aggregator.NO_GROUPING) {
            gbFieldType = null;
        } else {
            gbFieldType = child.getTupleDesc().getFieldType(gfield);
        }

        if (child.getTupleDesc().getFieldType(afield) == Type.STRING_TYPE) {
            ag = new StringAggregator(gfield, gbFieldType, afield, aop);
        } else {
            ag = new IntegerAggregator(gfield, gbFieldType, afield, aop);
        }
    }


    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link simpledb.execution.Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples If not, return
     * null;
     */
    public String groupFieldName() {
        return gField == Aggregator.NO_GROUPING ? null : it.getTupleDesc().getFieldName(0);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        if (gField == Aggregator.NO_GROUPING) {
            return it.getTupleDesc().getFieldName(0);
        }
        return it.getTupleDesc().getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    @Override
    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        child.open();
        super.open();
        while (child.hasNext()) {
            ag.mergeTupleIntoGroup(child.next());
        }
        it = ag.iterator();
        it.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (it.hasNext()) {
            return it.next();
        }
        return null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        it.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    @Override
    public TupleDesc getTupleDesc() {
        Type[] typeArray;
        String[] fieldArray;
        if (gField == Aggregator.NO_GROUPING) {
            typeArray = new Type[]{Type.INT_TYPE};
            fieldArray = new String[]{child.getTupleDesc().getFieldName(aField)};
        } else {
            typeArray = new Type[]{child.getTupleDesc().getFieldType(gField), Type.INT_TYPE};
            fieldArray = new String[]{child.getTupleDesc().getFieldName(gField), child.getTupleDesc().getFieldName(aField)};
        }
        return new TupleDesc(typeArray, fieldArray);
    }

    @Override
    public void close() {
        it.close();
        super.close();
        child.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }

}


