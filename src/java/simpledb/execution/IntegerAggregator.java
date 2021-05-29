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
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static class Item {
        public int max;
        public int min;
        public int num;
        public int sum;
        public int cnt;

        public Item() {
            this.cnt = 0;
            this.max = Integer.MIN_VALUE;
            this.min = Integer.MAX_VALUE;
            this.num = 0;
            this.sum = 0;
        }

    }

    private static final long serialVersionUID = 1L;
    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op what;
    private TupleDesc td;
    private String[] nameArray;
    private Map<Field, Item> itemMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.what = what;
        this.nameArray = new String[2];
        this.itemMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        IntField aggregateField = (IntField) tup.getField(aField);
        Field groupByField = (gbField == NO_GROUPING ? null : tup.getField(gbField));
        if (gbField == Aggregator.NO_GROUPING) {
            nameArray[0] = null;
        } else {
            nameArray[0] = tup.getTupleDesc().getFieldName(gbField);
        }
        nameArray[1] = tup.getTupleDesc().getFieldName(aField);
        int val = aggregateField.getValue();
        if (groupByField != null && !gbFieldType.equals(groupByField.getType())) {
            throw new IllegalArgumentException("Type of given tuple mismatch!");
        }

        switch (what) {
            case AVG:
                if (!itemMap.containsKey(groupByField)) {
                    Item item = new Item();
                    item.num = 1;
                    item.sum = val;
                    itemMap.put(groupByField, item);
                } else {
                    itemMap.get(groupByField).num++;
                    itemMap.get(groupByField).sum += val;
                }
                break;
            case MIN:
                if (!itemMap.containsKey(groupByField)) {
                    Item item = new Item();
                    item.min = Math.min(item.min, val);
                    itemMap.put(groupByField, item);
                } else {
                    itemMap.get(groupByField).min = Math.min(itemMap.get(groupByField).min, val);
                }
                break;

            case MAX:
                if (!itemMap.containsKey(groupByField)) {
                    Item item = new Item();
                    item.max = Math.max(item.max, val);
                    itemMap.put(groupByField, item);
                } else {
                    itemMap.get(groupByField).max = Math.max(itemMap.get(groupByField).max, val);
                }
                break;

            case SUM:
                if (!itemMap.containsKey(groupByField)) {
                    Item item = new Item();
                    item.sum += val;
                    itemMap.put(groupByField, item);
                } else {
                    itemMap.get(groupByField).sum += val;
                }
                break;

            case COUNT:
                if (!itemMap.containsKey(groupByField)) {
                    Item item = new Item();
                    item.cnt = 1;
                    itemMap.put(groupByField, item);
                } else {
                    itemMap.get(groupByField).cnt++;
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown Aggregate Operatiion!");
        }
    }


    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    @Override
    public OpIterator iterator() {
        return new IntAggregateIterator();
    }

    private class IntAggregateIterator implements OpIterator {
        private final List<Tuple> tuples;
        private Iterator<Tuple> it;

        public IntAggregateIterator() {
            tuples = new ArrayList<>();
            for (HashMap.Entry<Field, Item> it : itemMap.entrySet()) {
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

                switch (what) {
                    case AVG:
                        if (gbField == Aggregator.NO_GROUPING) {
                            t.setField(0, new IntField(it.getValue().sum / it.getValue().num));
                        } else {
                            t.setField(0, it.getKey());
                            t.setField(1, new IntField(it.getValue().sum / it.getValue().num));
                        }
                        break;
                    case MIN:
                        if (gbField == Aggregator.NO_GROUPING) {
                            t.setField(0, new IntField(it.getValue().min));
                        } else {
                            t.setField(0, it.getKey());
                            t.setField(1, new IntField(it.getValue().min));
                        }
                        break;

                    case MAX:
                        if (gbField == Aggregator.NO_GROUPING) {
                            t.setField(0, new IntField(it.getValue().max));
                        } else {
                            t.setField(0, it.getKey());
                            t.setField(1, new IntField(it.getValue().max));
                        }
                        break;


                    case SUM:
                        if (gbField == Aggregator.NO_GROUPING) {
                            t.setField(0, new IntField(it.getValue().sum));
                        } else {
                            t.setField(0, it.getKey());
                            t.setField(1, new IntField(it.getValue().sum));
                        }
                        break;


                    case COUNT:
                        if (gbField == Aggregator.NO_GROUPING) {
                            t.setField(0, new IntField(it.getValue().cnt));
                        } else {
                            t.setField(0, it.getKey());
                            t.setField(1, new IntField(it.getValue().cnt));
                        }
                        break;

                    default:
                        throw new IllegalArgumentException("Unknown Aggregate Operatiion!");
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

