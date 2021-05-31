package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int[] buckets;
    private final int min;
    private final int max;
    private final double width;
    private int ntuples = 0;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.min = min;
        this.max = max;
        this.buckets = new int[buckets];
        this.width = (max - min + 1.0) / buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int pos = lookForPos(v);
        if (pos != -1) {
            buckets[pos]++;
            ntuples++;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        double est = -1.0;
        switch (op) {
            case LESS_THAN:
//                handle edge condition
                if (v <= min) {
                    return 0.0;
                }
                if (v >= max) {
                    return 1.0;
                }

                int pos = lookForPos(v);
                double cnt = 0;
                for (int i = 0; i < pos; ++i) {
                    cnt += buckets[i];
                }
                cnt += buckets[pos] / width * (v - pos * width - min);
                est = cnt / ntuples;
                break;
            case LESS_THAN_OR_EQ:
                est = estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
                break;
            case GREATER_THAN:
                est = 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
                break;
            case GREATER_THAN_OR_EQ:
                est = estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
                break;
            case EQUALS:
                est = estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) - estimateSelectivity(Predicate.Op.LESS_THAN, v);
                break;
            case NOT_EQUALS:
                est = 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
                break;
            default:
                // simply do nothing here


        }
        return est;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        // what is avgSelectivity?
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        return "IntHistogram{" +
                "buckets=" + Arrays.toString(buckets) +
                ", min=" + min +
                ", max=" + max +
                ", width=" + width +
                ", ntuples=" + ntuples +
                '}';
    }


    //    helper functions
    private int lookForPos(int v) {
        if (v < min || v > max) {
            return -1;
        }
        return (int) ((v - min) / width);
    }
}
