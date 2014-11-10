package simpledb;

/**
 * Unique identifier for HeapPage objects.
 */
public class HeapPageId implements PageId {

	/**
	 * Table ID
	 */
	public int tableId;
	
	/**
	 * Page Number
	 */
	public int pgNo;
	
    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        this.tableId = tableId;
        this.pgNo = pgNo;
    }

    /**
     * @return the table associated with this PageId
     * 
     */
    public int getTableId() {
        return this.tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     * this PageId
     */
    public int pageNumber() {
 
        return this.pgNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     * the table number and the page number (needed if a PageId is used as a
     * key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
    	// Concatenate tableId and page number and return as int
    	int hash_pid = (int) ((this.tableId * Math.pow(10, ((Integer.valueOf(this.pgNo)).toString()).length()) + this.pgNo) % Math.pow(2, 15));
        return hash_pid;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     * ids are the same)
     */
    public boolean equals(Object o) {
    	if(o == null) {
    		return false;
    	}
    	// checks if the class is HeapPageId
    	if (o.getClass() == this.getClass()) {
    		HeapPageId page_2 = (HeapPageId) o;
    		// o is a HeapPageId, so check if page numbers and table ids are the same
    		if ((page_2.pgNo == this.pgNo) && (page_2.tableId == this.tableId)) {
    			return true;
    		}
    	}
        return false;
    }

    /**
     * Return a representation of this object as an array of
     * integers, for writing to disk.  Size of returned array must contain
     * number of integers that corresponds to number of args to one of the
     * constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}
