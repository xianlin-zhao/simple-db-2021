package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int numBucket;
    private int minValue;
    private int maxValue;
    private int[] histogram;
    private double width;
    private int count;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        numBucket = buckets;
        minValue = min;
        maxValue = max;
        int gap = max - min + 1;
        width = Math.max(gap * 1.0 / buckets, 1.0);
        histogram = new int[numBucket];
        count = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        count++;
        int valueId = (int) ((v - minValue) / width);
        histogram[valueId]++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        int valueId = (int) ((v - minValue) / width);
        if (op == Predicate.Op.EQUALS) {
            if (v < minValue || v > maxValue) {
                return 0.0;
            }
            return (histogram[valueId] / width) / count;
        } else if (op == Predicate.Op.NOT_EQUALS) {
            return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
        } else if (op == Predicate.Op.GREATER_THAN) {
            if (v < minValue) {
                return 1.0;
            } else if (v >= maxValue) {
                return 0.0;
            }
            double b_f = 1.0 * histogram[valueId] / count;
            double b_right = width * (valueId + 1.0);
            double b_part = (b_right - v) / width;
            int rightSum = 0;
            for (int i = valueId + 1; i < numBucket; i++) {
                rightSum += histogram[i];
            }
            return b_f * b_part + (rightSum * 1.0 / count);
        } else if (op == Predicate.Op.LESS_THAN) {
            if (v > maxValue) {
                return 1.0;
            } else if (v <= minValue) {
                return 0.0;
            }
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v) - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
        } else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
        } else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
        }
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
