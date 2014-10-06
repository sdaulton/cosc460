package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    
    public DbIterator child;
    public int aggfield;
    public int gbfield;
    public boolean group;
    public Aggregator.Op aggOp;
    public Aggregator aggregator;
    public TupleDesc td;
    public DbIterator agg_iter;
    /**
     * Constructor.
     * <p/>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.aggfield = afield;
        this.gbfield = gfield;
        this.aggOp = aop;
        this.group = false;
        this.td = this.child.getTupleDesc();
        Type gbfieldtype = null;
        if (gbfield != -1) {
        	group = true;
        	gbfieldtype = td.getFieldType(gbfield);
        }
        if (td.getFieldType(aggfield) == Type.INT_TYPE) {
        	this.aggregator = new IntegerAggregator(this.gbfield, gbfieldtype, aggfield, aggOp);
        } else {
        	this.aggregator = new StringAggregator(this.gbfield, gbfieldtype, aggfield, aggOp);
        }
        try {
        child.open();
        while(child.hasNext()) {
        	aggregator.mergeTupleIntoGroup(child.next());
        }
        child.close();
        agg_iter = aggregator.iterator();
        } catch (TransactionAbortedException e) {
        	throw new RuntimeException("Transaction aborted");
        } catch (DbException e) {
        	throw new RuntimeException("Transaction aborted");
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link simpledb.Aggregator#NO_GROUPING}
     */
    public int groupField() {
        if (group) {
        	return this.gbfield;
        }
        return simpledb.Aggregator.NO_GROUPING;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples If not, return
     * null;
     */
    public String groupFieldName() {
    	if (group) {
    		return td.getFieldName(gbfield);
    	}
    	return null;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return aggfield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return aggOp.toString() + " " + (td.getFieldName(aggfield));
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aggOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open();
        ((TupleIterator)agg_iter).open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        Tuple t = null;
        while (agg_iter.hasNext()) {
        	t = agg_iter.next();
        	return t;
        }
        return t;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	((TupleIterator) agg_iter).rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p/>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        return agg_iter.getTupleDesc();
    }

    public void close() {
        ((TupleIterator) agg_iter).close();
        super.close();
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length == 1) {
        	this.child = children[0];
        }
    }

}
