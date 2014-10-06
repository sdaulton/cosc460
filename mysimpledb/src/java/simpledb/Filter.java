package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    
    public DbIterator[] iter_children; //  array of child DbIterators -- if only one child, only 1 DbIterator in array
    public Predicate pred; //predicate for the filter

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.pred = p;
        this.iter_children = new DbIterator[1];
        this.iter_children[0] = child;
    }

    public Predicate getPredicate() {
        return pred;
    }

    public TupleDesc getTupleDesc() {
        return iter_children[0].getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	iter_children[0].open();
    	super.open();
    }

    public void close() {
    	iter_children[0].close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	iter_children[0].rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        Tuple t = null;
    	while (iter_children[0].hasNext()) {
        	t = iter_children[0].next();
        	if (pred.filter(t)) {
        		
        		return t;
        	}
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return iter_children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length == 1) {
        	iter_children[0] = children[0];
        } 
        // should I throw exception here???
    }

}
