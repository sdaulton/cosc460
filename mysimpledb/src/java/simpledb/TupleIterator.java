package simpledb;

import java.util.*;

/**
 * Implements a DbIterator by wrapping an Iterable<Tuple>.
 */
public class TupleIterator implements DbIterator {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    ListIterator<Tuple> i = null;
    TupleDesc td = null;
    ArrayList<Tuple> tuples;
    //Iterable<Tuple> tuples = null;
    public boolean open;

    /**
     * Constructs an iterator from the specified Iterable, and the specified
     * descriptor.
     *
     * @param tuples The set of tuples to iterate over
     */
    public TupleIterator(TupleDesc td, Iterable<Tuple> tuples) {
        this.open = false;
    	this.td = td;
        this.tuples = new ArrayList<Tuple>((Collection<Tuple>)tuples);
        // check that all tuples are the right TupleDesc
        for (Tuple t : tuples) {
            if (!t.getTupleDesc().equals(td)) {
                throw new IllegalArgumentException(
                        "incompatible tuple in tuple set");
        	}
        }
        i = this.tuples.listIterator();
    }

    public void open() {
    	this.open = true;
    }

    public boolean hasNext() {
        return i.hasNext();
    }

    public Tuple next() {
        return i.next();
    }

    public void rewind() {
    	if (open) {
    		while(i.hasPrevious()) {
    			i.previous();
    		}
        }
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void close() {
        this.open = false;
        
    }
}
