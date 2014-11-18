package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

	// the file backing this HeapFile on disk
	public final File f;
	
	// the TupleDesc for this HeapFile
	public final TupleDesc td;
	
	// unique id for this HeapFile
	public final int id;
	
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        this.id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	InputStream in;
    	try {
    		in = new FileInputStream(this.f);
    		int page_size = BufferPool.getPageSize();
    		//byte array to store the bytes of the page
    		byte[] page_byteAr = new byte[page_size];
    		// skip to the correct page
    		if (this.numPages() < pid.pageNumber()) {
    			in.close();
    			throw new RuntimeException("Page not in file.");
    			//throw new IllegalArgumentException("Page not in file.");
    		}
    		in.skip(pid.pageNumber() * page_size);
    		// Read the number of bytes in a page
    		in.read(page_byteAr, 0, page_size);
        	in.close();
        	return new HeapPage((new HeapPageId(pid.getTableId(), pid.pageNumber())), page_byteAr);
    	} catch (IOException e){
    		throw new RuntimeException("Couldn't read file");
    	}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile file = new RandomAccessFile(f, "rw");
        int pgNo = page.getId().pageNumber();
        int page_size = BufferPool.getPageSize();
        file.skipBytes(pgNo * page_size);
        file.write(page.getPageData());
        file.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	// size of the file / size of a page
        int num_pages = (int) Math.ceil(((float) this.f.length()) / BufferPool.getPageSize());
        return num_pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        int tableId = this.getId();
        BufferPool bp = Database.getBufferPool();
        HeapPage page = null;
        boolean page_has_space = false; // indicates if page has an open slot
        for (int i = 0; i < numPages(); i++) {
        	HeapPageId pid = new HeapPageId(tableId, i);
        	page = (HeapPage) bp.getPage(tid, pid, Permissions.READ_ONLY);
        	if (page.getNumEmptySlots() > 0) {
        		// page has an open slot
        		page = (HeapPage) bp.getPage(tid, pid, Permissions.READ_WRITE);
        		page_has_space = true;
        		break;
        	} else {
        		if (i != numPages()-1) {
        			bp.releasePage(tid, pid);
        		} else {
        			// IS THIS RIGHT??
        			// we are going to get a new page, make sure no one is writing to the page
        			// before the new page in the heapfile
        			// this hopefully ensures that no one can create a new page at the same time
        			// since they would first need to get a shared lock on this page and look 
        			// for empty tuple slots
        			page = (HeapPage) bp.getPage(tid, pid, Permissions.READ_WRITE);
        		}
        	}
        }
        if (page_has_space){
        	// a page has an open slot
        	page.insertTuple(t);
        } else {
        	// all pages are full -> create new page
        	int pgNo = numPages();
        	HeapPageId pid = new HeapPageId(tableId, pgNo);
        	HeapPage new_page = new HeapPage(pid, HeapPage.createEmptyPageData());
        	
        	OutputStream output = new BufferedOutputStream(new FileOutputStream(f, true), BufferPool.getPageSize());
        	output.write(new_page.getPageData(),0, BufferPool.getPageSize());
           	output.flush();
           	output.close();
           	page = (HeapPage) bp.getPage(tid, pid, Permissions.READ_WRITE);
           	page.insertTuple(t);
        }
        page.markDirty(true, tid);
        ArrayList<Page> modified_pages = new ArrayList<Page>();
        modified_pages.add(page);
        return modified_pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	HeapPageId t_pid = (HeapPageId) t.rid.pid;
    	if (t_pid.tableId != getId()) {
    		// tuple is not in file
    		throw new DbException("Tuple not in HeapFile.");
    	}
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.rid.pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);
        ArrayList<Page> modified_pages = new ArrayList<Page>();
        modified_pages.add(page);
        return modified_pages;
    }

    /**
     * Class for HeapFile Iterator.  Iterates through tuples in a Heapfile
     */
    class HeapFileIterator implements DbFileIterator {
    	
    	// Current page number
    	private int current_page_num;
    	
    	// Number of pages stored in this HeapFile
    	private int num_pages;
    	
    	// TupleIterator to iterate over the tuples in the file
    	private Iterator<Tuple> tuple_iter;
    	
    	// Transaction Id
    	private TransactionId transId;
    	
    	// Boolean indicating if the iterator is open or closed
    	private boolean isOpen;
    	
    	/**
         * Constructor for HeapFileIterator
         */
    	public HeapFileIterator(TransactionId tid) {
    		current_page_num = 0; 
    		num_pages = numPages();
    		transId = tid;
    		isOpen = false;
    		if (num_pages <= 0) {
    			throw new RuntimeException("No pages in Heapfile");
    		}
    	}
    	
    	/**
         * Sets the tuple iterator: creates a tuple iterator for the given page number
         *
         * @throws DbException when there are problems opening/accessing the database.
         */
    	private void set_tuple_iter() throws DbException, TransactionAbortedException {
    		if (current_page_num >= num_pages) {
    			throw new RuntimeException("No more pages in file.");
    		}
    			//create a HeapPageId for the current (next) page
    			HeapPageId pid = new HeapPageId(getId(), current_page_num);
    			//get the HeapPage via the BufferPool
    			BufferPool bp = Database.getBufferPool();
    			HeapPage page = (HeapPage) bp.getPage(transId, pid, Permissions.READ_ONLY);
    			//create a TupleIterator for the page
    			this.tuple_iter = page.iterator();
    	}
    	
    	/**
         * Opens the iterator
         *
         * @throws DbException when there are problems opening/accessing the database.
         */
        public void open() throws DbException, TransactionAbortedException {
        	if (tuple_iter == null){
        		set_tuple_iter();
        	}
        	// tuple_iter should not be null here
        	if (tuple_iter == null) {
        		throw new DbException("Could not set up tuple iterator.");
        	}
    		isOpen = true;
        }

        /**
         * @return true if there are more tuples available.
         */
        public boolean hasNext() throws DbException, TransactionAbortedException {
        	if (!isOpen) {
        		return false;
        	}
        	if (tuple_iter.hasNext()){
        		return true;
        	} else if (this.current_page_num + 1 < this.num_pages) {
        		this.current_page_num++;
            	set_tuple_iter();
            	open();
            	return this.hasNext();
        	}
        	return false;
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        	if (!isOpen) {
        		throw new NoSuchElementException("Iterator is closed.  Operation not supported when closed.");
        	}
        	if (this.hasNext()){
        		return tuple_iter.next();
        	} 
        	throw new NoSuchElementException("No more tuples in file.");
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException When rewind is unsupported.
         */
        public void rewind() throws DbException, TransactionAbortedException {
        	if (!isOpen) {
        		throw new DbException("Iterator is closed.  Operation not supported when closed.");
        	}
        	current_page_num = 0;
        	set_tuple_iter();
        }
        
        /**
         * Closes the iterator.
         */
        public void close() {
        	isOpen = false;
        }
    }
    
    
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

}

