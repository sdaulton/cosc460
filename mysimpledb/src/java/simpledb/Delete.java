package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    public TransactionId tid;
    public DbIterator child;
    boolean alreadyDeleted;
    Tuple retTuple; // tuple that the Delete returns
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
        this.child = child;
        this.alreadyDeleted = false;
        Type[] typeAr = new Type[]{Type.INT_TYPE};
        String[] fieldAr = new String[]{"numDeleted"};
        TupleDesc td = new TupleDesc(typeAr, fieldAr);
        this.retTuple = new Tuple(td);
    }

    public TupleDesc getTupleDesc() {
    	return this.retTuple.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	this.child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (alreadyDeleted) {
        	return null;
        }
    	int count = 0;
    	BufferPool bp = Database.getBufferPool();
        while (child.hasNext()) {
        	try {
				bp.deleteTuple(this.tid, child.next());
			} catch (IOException e) {
				throw new DbException("Unable to delete tuple from table");
			}
        	count++;
        	
        }
        // return tuple containing number of tuples deleted
        retTuple.setField(0, new IntField(count));
        this.alreadyDeleted = true;
        return retTuple;
    }

    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	if (children.length == 1) {
        	this.child = children[0];
        }
    }

}
