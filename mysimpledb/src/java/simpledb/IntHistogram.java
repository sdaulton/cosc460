package simpledb;

import simpledb.Predicate.Op;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * <p/>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p/>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p/>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
	private double[] histogram;
	private int numBuckets; //number of buckets
	private int min;
	private int max;
	private int std_width; // standard bucket width
	private int lst_width; // last bucket's width
	private int numTuples; // number of tuples in the table
	
    public IntHistogram(int buckets, int min, int max) {
        if (max - min + 1 < buckets) {
        	// number of buckets > number of possible distinct values
        	this.numBuckets = max - min + 1;
        	this.std_width = 1;
        	this.lst_width = 1;
        } else {
        	this.numBuckets = buckets;
        	this.std_width = (max - min + 1) / buckets;
            if (this.std_width == 0) {
            	this.std_width = 1;
            }
            this.lst_width = (max - min + 1) % (buckets) + this.std_width;
        }
    	this.histogram = new double[numBuckets];
        this.min = min;
        this.max = max;
        this.numTuples = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int bucket_idx = (int) Math.floor((v - min) / std_width);
        if (max - lst_width < v) {
    		bucket_idx = numBuckets - 1;
    	}
        if ((v < min) || (v >max)) {
        	throw new RuntimeException("Value " + v + " is not in range covered by histogram");
        }
        histogram[bucket_idx] += 1.0;
        numTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p/>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	int bucket_idx = (int) Math.floor((v - min) / std_width);
    	if (max - lst_width < v) {
    		bucket_idx = numBuckets - 1;
    	}
    	double width = 0.0;
    	double b_right = 0.0;
    	double b_left = min + bucket_idx * std_width;
    	if (bucket_idx < this.numBuckets - 1) {
    		// not last bucket
    		width = std_width;
    		b_right = min + (bucket_idx + 1) * width;
    	} else {
    		// last bucket
    		width = lst_width;
    		b_right = max+1;
    	}
    	
    	double b_part_greater = 0.0;
    	double select_greater = 0.0;
    	double b_part_equal = 0.0;
    	double select_equal = 0.0;
    	double b_part_less = 0.0;
    	double select_less = 0.0;
    	if((v >= min) && (v <= max)) {
	    	// calculate selectivity for tuples > v
	    	b_part_greater = (b_right - 1 - v) / width;
	    	select_greater = b_part_greater * histogram[bucket_idx] / numTuples;
	    	for (int i = bucket_idx + 1; i < numBuckets; i++) {
				select_greater += histogram[i] / numTuples;
			}
	    	
	    	// calculate selectivity for tuples = v
	    	b_part_equal = 1.0 / width;
	    	select_equal = b_part_equal * histogram[bucket_idx] / numTuples;
	    	
	    	// calculate selectivity for tuples < v
	    	b_part_less = (v - b_left) / width;
	    	select_less = b_part_less * histogram[bucket_idx] / numTuples;
	    	for (int i = bucket_idx - 1; i >= 0; i--) {
				select_less += histogram[i] / numTuples;
			}
    	}
    	
    	if ((op == Op.EQUALS) || (op == Op.LIKE)) {
    		if ((v < min) || (v > this.max)) {
            	return 0.0;
            }
    		
    		return select_equal;
    	} else if (op == Op.GREATER_THAN) {
    		if (v < min) {
            	return 1.0;
            } else if(v >= max) {
            	return 0.0;
            } else {
            	return select_greater;
            }
    	} else if (op == Op.LESS_THAN) {
    		if (v <= min) {
            	return 0.0;
            } else if(v > max) {
            	return 1.0;
            } else {
            	return select_less;
            }
    	} else if (op == Op.LESS_THAN_OR_EQ) {
    		if (v < min) {
            	return 0.0;
            } else if(v >= max) {
            	return 1.0;
            } else {
            	return select_less + select_equal;
            }
    	} else if (op == Op.GREATER_THAN_OR_EQ) {
    		if (v <= min) {
            	return 1.0;
            } else if(v > max) {
            	return 0.0;
            } else {
            	return select_greater + select_equal;
            }
		} else if (op == Op.NOT_EQUALS) {
			if ((v < min) || (v > max)) {
            	return 1.0;
            }
    		return 1.0 - select_equal;
		}
        return -1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return "Histogram with " + numBuckets + "buckets.";
    }
}
