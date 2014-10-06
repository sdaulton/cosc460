package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    public TransactionId tid;
    public DbIterator child;
    int tableId;
    boolean alreadyInserted;
    Tuple retTuple; // tuple that the Insert returns
    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        this.tid = t;
        this.tableId = tableid;
        if (!Database.getCatalog().getTupleDesc(tableId).equals(child.getTupleDesc())) {
        	throw new DbException("The schema of the tuples being inserted does not match the schema of the table.");
        }
        this.child = child;
        this.alreadyInserted = false;
        Type[] typeAr = new Type[]{Type.INT_TYPE};
        String[] fieldAr = new String[]{"numInserted"};
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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (alreadyInserted) {
        	return null;
        }
    	int count = 0;
    	BufferPool bp = Database.getBufferPool();
        while (child.hasNext()) {
        	try {
				bp.insertTuple(this.tid, this.tableId, child.next());
			} catch (IOException e) {
				throw new DbException("Unable to add tuple to table");
			}
        	count++;
        	
        }
        // return tuple containing number of tuples inserted
        retTuple.setField(0, new IntField(count));
        this.alreadyInserted = true;
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
