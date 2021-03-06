package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    
    /**
     *  The schema of this tuple
     */
    public final TupleDesc td;
    
    /**
     *  The RecordId of this tuple
     */
    public RecordId rid;
    
    /**
     *  The array of fields containing the data in this tuple
     */
    public Field[] fields;
    
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        if (td.numFields() < 1) {
        	throw new RuntimeException("Given Tuple Descriptor has 0 fields");
        }
    	this.td = td;
    	this.fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
    	return this.td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        return this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }

    
    /**
     * Check if index is valid, i.e. in range for the this tuple.
     * (helper function)
     * 
     * @param i index to check
     */
    private boolean checkIndex(int i) {
    	if ((i >= 0) && (i < this.fields.length)) {
    		return true;
    	}
    	return false;
    }
    
    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        if (checkIndex(i)) {
        	// index is valid, so check if new field has same type as listed in schema
        	if (f.getType() == this.td.getFieldType(i)) {
        		this.fields[i] = f;
        	} else {
        		throw new RuntimeException("New field must be same data type as described in schema");
        	}
        	//index is invalid
        } else {
        	throw new RuntimeException("Invalid index: index must be in range");
        } 
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
    	if (checkIndex(i)) {
    		// valid index
    		return this.fields[i];
    	}
    	// invalid index
    	throw new RuntimeException("Invalid index: index must be in range");
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p/>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p/>
     * where \t is any whitespace, except newline
     */
    public String toString() {
        String fields_str = "";
        //concatenate contents of tuple to fields_str, where each column is separated by tabs
    	for (int i = 0; i < this.fields.length - 1; i++) {
        	fields_str += fields[i].toString() + "\t";
        }
    	// add the contents of the last field without a tab after it
    	fields_str += fields[this.fields.length - 1]; 
    	return fields_str;
    }

}
