 package simpledb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

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

    public int gbfield;
    public Type gbfieldtype;
    public boolean group;
    public int aggfield;
    public Op aggOp;
    public TupleDesc td;
    public Type[] typeAr;
    public String[] fieldAr;
    public HashMap<String, Integer> countsMap;
    public HashMap<String, Tuple> tupMap;
    
    
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.aggfield = afield;
        this.aggOp = what;
        this.typeAr = null;
        this.fieldAr = null;
        if (this.gbfieldtype == null) {
        	this.group = false;
        } else {
        	this.group = true;
        }
        if (!group) {
        	typeAr = new Type[1];
        	typeAr[0] = Type.INT_TYPE;
        	fieldAr = new String[1];
        	fieldAr[0] = aggOp.toString();
        } else {
        	typeAr = new Type[2];
        	typeAr[0] = gbfieldtype;
        	typeAr[1] = Type.INT_TYPE;
        	fieldAr = new String[2];
        	fieldAr[0] = null;
        	fieldAr[1] = aggOp.toString();
        	
        }
        tupMap = new HashMap<String, Tuple>();
        if (aggOp == Aggregator.Op.AVG) {
    		this.countsMap = new HashMap<String, Integer>();
    	}
        
        this.td = null;
        this.tupMap = new HashMap<String, Tuple>();
        
    }

    /**
     * Helper function that performs the aggregate operation on the newT and the current aggregated tuple (the result)
     *
     * @param tup the Tuple containing an aggregate field and a group-by field (the new tuple)
     * @param agg, the current aggregate tuple result for the group
     * @return the aggregate tuple result for the group
     */
    private Tuple performOp(Tuple agg, Tuple newT, String key) {
    	Field f;
    	int i = 0;
    	if (this.td == null) {
    		if (group) {
    			fieldAr[0] = newT.getTupleDesc().getFieldName(gbfield);
    			i = 1;
    		}
    		fieldAr[i] = aggOp.toString() + " " + (newT.getTupleDesc().getFieldName(aggfield));
    		this.td = new TupleDesc(this.typeAr, this.fieldAr);
    	}
    	int aggIdx = td.numFields()-1;
    	int next_int = ((IntField) newT.getField(aggfield)).getValue();
    	if (agg == null) {
    		//haven't seen this group yet, or this is the first tuple in the agg
    		agg = new Tuple(this.td);
    		
    		if (aggOp == Aggregator.Op.COUNT) {
    			// set count to 1
    			f = new IntField(1);
    		} else {
    			// set agg to the value to newT
    			f = new IntField(next_int);
    		}
    		if (group) {
    			// set the grouping
    			agg.setField(0, newT.getField(gbfield));
    		}
    	}
    	else {
    		int agg_int = ((IntField) agg.getField(aggIdx)).getValue();
    		if (aggOp == Aggregator.Op.COUNT) {
    			agg_int++;
    			
    		} else if (aggOp == Aggregator.Op.SUM) {
    			agg_int += next_int;
    		} else if (aggOp == Aggregator.Op.MIN) {
    			if (agg_int > next_int) {
    				agg_int = next_int;
    			}
    		} else if (aggOp == Aggregator.Op.MAX) {
    			if (agg_int < next_int) {
    				agg_int = next_int;
    			}
    		} else {
    			//AVG
    			int count = countsMap.get(key).intValue();
    			agg_int = (agg_int * (count -1) + next_int)/count;
    		}
    		f = new IntField(agg_int);
    	}
    	agg.setField(aggIdx, f);
    	return agg;
    	
    }
    
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Tuple val = null;
    	String key;	
    	if (!group) {
    		key = "0";
    		
    	} else {
    		if (this.gbfieldtype == Type.INT_TYPE) {
    			key = ((IntField) tup.getField(this.gbfield)).toString();
    		} else {
    			key = ((StringField) tup.getField(this.gbfield)).getValue();
    		}
    	}
		if(tupMap.containsKey(key)) {
			// we have already encountered a tuple in this group
			val = tupMap.remove(key);
			if (aggOp == Aggregator.Op.AVG) {
    			int groupCount = countsMap.remove(key).intValue() + 1;
    			countsMap.put(key, groupCount);
    		}
			val = performOp(val, tup, key);
			 
		} else {
			// a group we haven't encounter yet
			if (aggOp == Aggregator.Op.AVG) {
	    		countsMap.put(key, 1);
	    	}
			val = performOp(val, tup, key);
	    	
		}
		tupMap.put(key, val);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public DbIterator iterator() {
    	Collection<Tuple> tuples = tupMap.values();
    	//System.out.println(tupMap.values().size());
    	//Object[] test = tupMap.values().toArray();
    	//for(int i = 0; i < tupMap.values().size(); i++) {
    	//	System.out.println(test[i].toString());
    	//}
    	//TupleIterator test = new TupleIterator(this.td, tuples);
    	//while (test.hasNext()) {
    	//	System.out.println(test.next().toString());
    	//}
    	//System.out.println("END");
        return new TupleIterator(this.td, tuples);
        /*throw new
        UnsupportedOperationException("please implement me for lab3");    
        */

    }

}
