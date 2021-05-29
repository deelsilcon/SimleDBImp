package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op what;
    private TupleDesc td;
    private String[] nameArray;
    private Map<Field, Integer> countMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.what = what;
        this.nameArray = new String[2];
        this.countMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupByField = (gbField == NO_GROUPING ? null : tup.getField(gbField));
        if (gbField == Aggregator.NO_GROUPING) {
            nameArray[0] = null;
        } else {
            nameArray[0] = tup.getTupleDesc().getFieldName(gbField);
        }
        nameArray[1] = tup.getTupleDesc().getFieldName(aField);
        if (groupByField != null && !gbFieldType.equals(groupByField.getType())) {
            throw new IllegalArgumentException("Type of given tuple mismatch!");
        }

        if (!countMap.containsKey(groupByField)) {
            if (what != Op.COUNT) {
                throw new IllegalArgumentException("Only COUNT() is supported in STRING!");
            }
            countMap.put(groupByField, 1);
        } else {
            Integer i = countMap.get(groupByField) + 1;
            countMap.put(groupByField, i);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    @Override
    public OpIterator iterator() {
        return new StringAggregateIterator();
    }

    private class StringAggregateIterator implements OpIterator {
        private final List<Tuple> tuples;
        private Iterator<Tuple> it;

        public StringAggregateIterator() {
            tuples = new ArrayList<>();
            for (HashMap.Entry<Field, Integer> it : countMap.entrySet()) {
                Type[] typeArray;
                String[] fieldArray;
                if (gbField == Aggregator.NO_GROUPING) {
                    typeArray = new Type[]{Type.INT_TYPE};
                    fieldArray = new String[]{nameArray[1]};
                } else {
                    typeArray = new Type[]{gbFieldType, Type.INT_TYPE};
                    fieldArray = new String[]{nameArray[0], nameArray[1]};
                }
                td = new TupleDesc(typeArray, fieldArray);
                Tuple t = new Tuple(td);
                if (gbField == Aggregator.NO_GROUPING) {
                    t.setField(0, new IntField(it.getValue()));
                } else {
                    t.setField(0, it.getKey());
                    t.setField(1, new IntField(it.getValue()));
                }
                tuples.add(t);
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            it = tuples.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.close();
            this.open();
        }


        @Override
        public TupleDesc getTupleDesc() {
            return td;
        }

        @Override
        public void close() {
            it = null;
        }
    }
}
