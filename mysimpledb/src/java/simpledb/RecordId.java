package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    
    /**
     * PageId for this record
     */
    public PageId pid;
    
    /**
     * Tuple number for this record
     */
    public int tupleno;
    
    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     *
     * @param pid     the pageid of the page on which the tuple resides
     * @param tupleno the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.pid = pid;
        this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        return this.tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return this.pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     *
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
    	if(o == null) {
    		return false;
    	}
    	// checks if the class is RecordId
    	if (o.getClass() == this.getClass()) {
    		RecordId record_2 = (RecordId) o;
    		// o is a RecordId, so check if page ids and tuple numbers are the same
    		if ((record_2.pid.equals(this.pid)) && (record_2.tupleno == this.tupleno)) {
    			return true;
    		}
    	}
        return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     *
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // Get the hashCode for the Page Id
    	int hash_pid = this.pid.hashCode();
        // Concatenate hashCode for the Page Id with the tuple number
    	// Thus, the hashcode for the Record Id is table ID concatenated with 
    	// Page Number, concatenated with tuple number
    	String hash_rid = "" + hash_pid + this.tupleno;
        return Integer.parseInt(hash_rid);

    }

}
