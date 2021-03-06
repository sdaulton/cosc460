package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    /**
     * The transaction this scan is running as a part of.
     */
    private final TransactionId tid;
    
    /**
     * the table to scan
     */
    private final int tableId;
    
    /**
     * the alias of this table (needed by the parser)
     */
    private final String tableAlias;
    
    private DbFileIterator it;
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        this.it =  Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return this.tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        it.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
    	// get the TupleDesc
    	TupleDesc td = Database.getCatalog().getTupleDesc(this.tableId);
    	// create an iterator over the TDItems in the TupleDesc
    	int num_fields = td.numFields();
    	// Type and field arrays for new Tuple Desc
    	Type[] typeAr = new Type[num_fields];
    	String[] fieldAr = new String[num_fields];
    	
    	Iterator<TupleDesc.TDItem> td_it = td.iterator();
    	int i = 0;
    	TupleDesc.TDItem next;
    	while (td_it.hasNext()) {
    		//read field and add tableAlias to it
    		next = td_it.next();
    		typeAr[i] = next.fieldType;
    		fieldAr[i] = this.tableAlias + "." + next.fieldName;
    		i++;
    	}
    	return new TupleDesc(typeAr, fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return it.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return it.next();
    }

    public void close() {
        it.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
       it.rewind();
    }
}
