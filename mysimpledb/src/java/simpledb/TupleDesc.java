package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private static final long serialVersionUID = 1L;

    /**
     * An array of TDItems, the descriptor for the tuple
     */
    public TDItem[] schema;
    
    
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	if (typeAr.length < 1) {
    		throw new RuntimeException("TupleDesc constructor requires at least one type, none given");
    	}
    	// Create array of TDItems
    	schema = new TDItem[typeAr.length]; 
    	for (int i = 0; i < typeAr.length; i++) {
    		schema[i] = new TDItem(typeAr[i], fieldAr[i]);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	if (typeAr.length < 1) {
    		throw new RuntimeException("TupleDesc constructor requires at least one type, none given");
    	}
    	// Create array of TDItems
    	schema = new TDItem[typeAr.length]; 
    	for (int i = 0; i < typeAr.length; i++) {
    		// set all field names as null, since none were given
    		schema[i] = new TDItem(typeAr[i], null);
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.schema.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	if ((i < this.schema.length) && (i >= 0)) {
    		return this.schema[i].fieldName;
    	}
    	throw new NoSuchElementException("Index i is out of range of the table schema");
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if ((i < this.schema.length) && (i >= 0)) {
    		return this.schema[i].fieldType;
    	}
    	throw new NoSuchElementException("Index is out of range of the table schema");
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null) {
        	throw new NoSuchElementException("Field name must be a string, not null");
        }
    	for (int i = 0; i < this.schema.length; i++) {
        	if (name.equals(this.schema[i].fieldName)) {
        		return i;
        	}
        }
        throw new NoSuchElementException("There is no field with given name in table schema");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int total_bytes = 0;
    	for (int i = 0; i < this.schema.length; i++) {
    		// adds the fixed length of type stored in ith field of tuple
    		total_bytes += this.schema[i].fieldType.getLen(); 
        }
    	return total_bytes;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	int td1_length = td1.numFields();
    	int td2_length = td2.numFields();
        //create a new array of types and a new array of names to pass to the TupleDesc constructor
    	Type[] typeAr = new Type[td1_length + td2_length];
        String[] fieldAr = new String[td1_length + td2_length];
        // add the field names and types from td1
        for(int i = 0; i < td1_length; i++)
        {
        	typeAr[i] = td1.getFieldType(i);
        	fieldAr[i] = td1.getFieldName(i);	
        }
        //then add the field names and types from td2
        for(int i = 0; i < td2_length; i++)
        {
        	typeAr[i + td1_length] = td2.getFieldType(i);
        	fieldAr[i + td1_length] = td2.getFieldName(i);	
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        //some code goes here
    	if(o == null) {
    		return false;
    	}
    	// checks if the class is TupleDesc
    	if (o.getClass() == this.getClass()) {
    		TupleDesc td1 = (TupleDesc) o;
    		// o is a TupleDesc, so check its length
    		if (this.numFields() != td1.numFields()) {
    			return false;
    		} else {
    			// check if both TupleDescs have the same field type for each field i
    			for (int i = 0; i < this.numFields(); i++) {
    				if (td1.getFieldType(i) != this.getFieldType(i)) {
    					return false;
    				}
    			}
    		}
    		return true;
    	}
    	return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])"
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        String td_str = "";
        // Convert each TDItem to a String concatenate them to td_str, with each TDItem String separated
        // by a comma
        for (int i = 0; i < this.numFields()- 1; i++) {
        	td_str += this.schema[i].toString() + ", ";
        }
        // add the last TDItem string, with no following comma
        td_str += this.schema[this.numFields() - 1].toString();
        return td_str;
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return Arrays.asList(this.schema).iterator();
    }

}
