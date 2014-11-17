package simpledb;

import java.io.*;

import java.util.concurrent.ConcurrentHashMap;

import java.util.ArrayList; //I ADDED THIS, AM I ALLOWED TO?
import java.util.LinkedList; //AND THIS?
/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p/>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    public static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Maximum number of pages allowed in the buffer pool
     */
    public int numPages;
    
    /**
     * Current number of pages in the buffer pool
     */
    public int pageCount;
    
    /**
     * Array of Pages in the buffer pool
     */
    public Page[] pages;
    
    /**
     * LinkedList of pages in the buffer pool, the head of the list is the most recently used page, the tail is the least
     */
    public LinkedList<PageId> evict_list; 
    
    /**
     * LockManager for the DB
     */
    private LockManager  lockManager;
    
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pageCount = 0;
        this.pages = new Page[numPages];
        this.evict_list = new LinkedList<PageId>();
        this.lockManager = new LockManager(this.numPages);
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p/>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // get lock for the page
    	if (perm.permLevel == 0) {
    		// shared
    		lockManager.acquirePageLock(tid, pid, false);
    	} else {
    		// exclusive
    		lockManager.acquirePageLock(tid, pid, true);
    	}
    		
    	for (int i = 0; i < this.numPages; i++) {
        	// If the page is in the BufferPool, return it
        	
        	if (this.pages[i] != null){
        		if (this.pages[i].getId().equals(pid)) {
        			evict_list.remove(pid); // remove the instance of pid in the linked list
        			evict_list.addFirst(pid); // add the instance of pid to the head of the linked list
        			return this.pages[i];
        		}
        	}
        }
        if (this.pageCount >= this.numPages) {
        	evictPage();
        	this.pageCount -= 1;
        }
        // Page is not in BufferPool, get its table id
        Catalog catalog = Database.getCatalog();
        DbFile file = catalog.getDatabaseFile(pid.getTableId());
        int first_empty = 0;
        for (int i = 0; i < numPages; i++) {
        	if (pages[i] == null) {
        		first_empty = i;
        		break;
        	}
        }
        this.pages[first_empty] = file.readPage(pid);
        this.pageCount++;
        evict_list.addFirst(pid);
        return this.pages[first_empty];
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        lockManager.releasePageLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p, false);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    
    /**
     * Helper function for insertTuple and Delete Tuple:
     * Gets the file, inserts/deletes the tuple, dirties the pages that were modified, and updates those pages if they are in the BufferPool
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     * @param isInsert a boolean indicating if the update is an Insert
     * 
     */
    public void updateTuple(TransactionId tid, int tableId, Tuple t, boolean isInsert) 
    		throws DbException, IOException, TransactionAbortedException {
    	Catalog catalog = Database.getCatalog();
        DbFile file = catalog.getDatabaseFile(tableId);
        ArrayList<Page> modified_pages = null;
        if (isInsert) {
        	modified_pages = file.insertTuple(tid, t);
        } else {
        	modified_pages = file.deleteTuple(tid, t);
        }
        Page page = null;
        for (int i = 0; i < modified_pages.size(); i++) {
        	page = modified_pages.get(i);
        	if (page != null) {
        		page.markDirty(true, tid);
        		for (int j = 0; j < numPages; j++) {
        			if (pages[j] != null) {
        				if (pages[j].getId().equals(page.getId())) {
        				// the modified page has a former version in the buffer pool
        				// replace with the modified page
        				pages[j] = page;
        				}
        			}
        		}
        	}
        }
    }
    
    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed until lab5).                                  // cosc460
     * May block if the lock(s) cannot be acquired.
     * <p/>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	updateTuple(tid, tableId, t, true);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p/>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	updateTuple(tid, t.getRecordId().getPageId().getTableId(), t, false);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	Catalog catalog = Database.getCatalog();
    	DbFile file = null;
    	Page page = null;
    	for (int i = 0; i < numPages; i++){
    		if (pages[i] != null) {
    			page = pages[i];
    			if (page.isDirty() != null) {
    				page.markDirty(false, new TransactionId());
    				file = catalog.getDatabaseFile(page.getId().getTableId());
    				file.writePage(page);
    			}
    			break;
    			
		
    		}
    	}
    	
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab6                                                                            // cosc460
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	Catalog catalog = Database.getCatalog();
    	DbFile file = catalog.getDatabaseFile(pid.getTableId());
    	Page page = null;
    	for (int i = 0; i < numPages; i++){
    		if (pages[i] != null) {
    			page = pages[i];
    			if (pages[i].getId().equals(pid)) {
    				if (page.isDirty() != null) {
    					page.markDirty(false, null);
    					file.writePage(page);
    				}
    				break;
    			}
		
    		}
    	}
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4                                                         // cosc460
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	PageId pid = evict_list.removeLast();
    	try{
    		flushPage(pid);
    	} catch (IOException e) {
    		throw new DbException("Could not write page to file.  Page not evicted.");
    	}
    	Page page = null;
    	for (int i = 0; i < numPages; i++){
    		if (pages[i] != null) {
    			page = pages[i];
    			if (pages[i].getId().equals(pid)) {
    				pages[i] = null;
    			}
    		}
    	}
    }

}
