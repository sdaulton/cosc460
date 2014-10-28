package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p/>
 * This class is not needed in implementing lab1|lab2|lab3.                                                   // cosc460
 */
class Stats {
	public IntHistogram iHist;
	public StringHistogram sHist;
	public int num_distinct;
	
	
}
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;
    

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(HashMap<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    
    private Stats[] stats;
    private int num_tuples;
    private double num_pages;
    private int ioCostPerPage;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	DbFile tableFile = Database.getCatalog().getDatabaseFile(tableid);
    	TupleDesc td = tableFile.getTupleDesc();
    	int tupleSize = td.getSize();
    	this.ioCostPerPage = ioCostPerPage;
    	HashMap<Integer, HashSet<Integer>> iDistinct= new HashMap<Integer, HashSet<Integer>>();
    	HashMap<Integer, HashSet<String>> sDistinct= new HashMap<Integer, HashSet<String>>();
    	int num_columns = td.numFields();
    	int[] mins = new int[num_columns];
    	int[] maxs = new int[num_columns];
    	
    	TransactionId tid = new TransactionId();
    	DbFileIterator t = tableFile.iterator(tid);
    	this.num_tuples = 0;
    	try {
    		t.open();
    	
    		Tuple tup = null;
    		while (t.hasNext()) {
    			tup = t.next();
    			this.num_tuples++;
    			for (int i = 0; i < num_columns; i++){
    				if (td.getFieldType(i) == Type.INT_TYPE) {
    					mins[i] = Math.min(((IntField)tup.getField(i)).getValue(), mins[i]);
    					maxs[i] = Math.max(((IntField)tup.getField(i)).getValue(), maxs[i]);
    				}
    			}
    		}
    		this.num_pages = num_tuples / (BufferPool.getPageSize() / td.getSize());
	    	this.stats = new Stats[num_columns];
	    	for (int idx = 0; idx < num_columns; idx++) {
	    		stats[idx] = new Stats();
	    		if (td.getFieldType(0) == Type.INT_TYPE) {
	    			stats[idx].iHist = new IntHistogram(NUM_HIST_BINS, mins[idx], maxs[idx]);
	    			stats[idx].sHist = null;
	    		} else {
	    			stats[idx].sHist = new StringHistogram(NUM_HIST_BINS);
	    			stats[idx].iHist = null;
	    		}
	    		
	    	}
	    	t.rewind();
	    	int val = 0;
	    	String s = "";
	    	while (t.hasNext()) {
    			tup = t.next();
    			for (int i = 0; i < num_columns; i++){
    				if (stats[i].iHist != null) {
    					val = ((IntField)tup.getField(i)).getValue();
    					stats[i].iHist.addValue(val);
    					if (iDistinct.get(i) == null) {
    						iDistinct.put(i, new HashSet<Integer>());
    					}
    					iDistinct.get(i).add(val);
    					
    				} else {
    					s = ((StringField)tup.getField(i)).getValue();
    					stats[i].sHist.addValue(s);
    					if (sDistinct.get(i) == null) {
    						sDistinct.put(i, new HashSet<String>());
    					}
    					sDistinct.get(i).add(s);
    					
    				}
    			}
    		}
	    	t.close();
	    	
    	} catch (TransactionAbortedException e) {
    		throw new RuntimeException("DbFileIterator failure");
    	} catch (DbException e2) {
    		throw new RuntimeException("DbFileIterator failure");
    	}
    	HashSet<Integer> distinct_i = null;
    	for (int i =0; i < num_columns; i ++) {
    		if ((distinct_i = iDistinct.get(i)) != null) {
    			stats[i].num_distinct = distinct_i.size();
    		} else {
    			stats[i].num_distinct = sDistinct.get(i).size();
    		}
    	}
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p/>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return this.num_pages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (this.num_tuples * selectivityFactor);
    }

    /**
     * This method returns the number of distinct values for a given field.
     * If the field is a primary key of the table, then the number of distinct
     * values is equal to the number of tuples.  If the field is not a primary key
     * then this must be explicitly calculated.  Note: these calculations should
     * be done once in the constructor and not each time this method is called. In
     * addition, it should only require space linear in the number of distinct values
     * which may be much less than the number of values.
     *
     * @param field the index of the field
     * @return The number of distinct values of the field.
     */
    public int numDistinctValues(int field) {
        return stats[field].num_distinct;

    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	if (constant.getType() == Type.INT_TYPE) {
    		return stats[field].iHist.estimateSelectivity(op, ((IntField)constant).getValue());
    	} else {
    		return stats[field].sHist.estimateSelectivity(op, ((StringField)constant).getValue());
    	}
        
    }

}
