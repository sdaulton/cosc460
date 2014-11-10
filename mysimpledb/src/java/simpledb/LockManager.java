package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

public class LockManager {
	
	static class LockNode {
		public boolean inUse;
		private final TransactionId tid;
		private final PageId pid;
		
		//constructor
		public LockNode(TransactionId tid, PageId pid) {
			this.tid = tid;
			this.pid = pid;
			this.inUse = false;
		}
		
		//returns true if the txn has the lock for the page
		public boolean hasLock() {
			return this.inUse;
		}
		
		//gives this txn the lock for the page
		public void getLock() {
			assert(inUse == false);
			inUse = true;
		}
		
		// Returns the TransactionId
		public TransactionId getTransactionId() {
			return this.tid;
		}
		// Returns the PageId
		public PageId getPageId() {
			return this.pid;
		}
		
	}
	//Ideas: create a lock for the lock manager - txns must get this lock to acquire/
	//release page locks, as soon as they acquire/release a page lock, they release
	// the lock manager lock
	
	
	
	// need data structure to store, transaction id and page for all locks currently held
	// need queue for each lock in use of transactions waiting for the lock
	private boolean inUse;
	private HashMap<PageId, LinkedList<LockNode>> lockTable;
	
	public LockManager(int numPages) {
		this.inUse = false;
		// make the size of the lock table equal to the number of pages in the bufferpool
		// hash map: maps pageId -> a LinkedList of LinkedLists where there is a linked list for each
		// unique PageId in the bucket.  The Linked list is the queue of nodes waiting
		// for the lock for that PageId.  The head of the LinkedList is the transaction
		// currently holding the node
		this.lockTable = new HashMap<PageId, LinkedList<LockNode>>(numPages);
	}
    
	//used to acquire the lock for the lock manager
    private void acquireLockManagerLock() {
    	boolean waiting = true;
    	while (waiting) {
    		
    		synchronized(this) {
    			if (!this.inUse) {
    				this.inUse = true;
    				waiting = false;
    			}
    		}
    		if (waiting) {
    			try {
    			Thread.sleep(1);
    			} catch (InterruptedException ignored) {}
    		}
    	}
    }
    
    private synchronized void releaseLockManagerLock() {
        this.inUse = false;
    }
    
    // this is the only method called by a transaction to request the lock for a page
    public void acquirePageLock(TransactionId t, PageId p) {
    	// get lock for LockManager
    	acquireLockManagerLock();
    	// now has lock for lock manager
    	boolean lockHeld = false;
    	lockHeld = requestLock(t,p);
    	// request has been entered
    	releaseLockManagerLock();
    	if (!lockHeld) {
    		// waiting for lock
    		waitForLock(t,p);
    	}
    	// txn has lock
    }
    
    // used by a txn to request a lock for a particular page
    // returns true if the txn now holds the lock for that page, and false if
    // is waiting for that lock
    private boolean requestLock(TransactionId t, PageId p) {
    	LinkedList<LockNode> pageQ = lockTable.get(p);
    	if (pageQ == null) {
    		// lock for page p is not in use
    		pageQ = new LinkedList<LockNode>();
    		LockNode txn = new LockNode(t,p);
    		txn.getLock();
    		pageQ.addLast(txn);
    		lockTable.put(p, pageQ);
    		//return some value indicating lock was awarded
    		return true;
    	} else if ((!pageQ.isEmpty()) && (pageQ.getFirst().tid.equals(t))) {
    		// transaction t already has the lock
    		return true;
    	} else {
    		Iterator<LockNode> txnIter = pageQ.iterator();
    		while (txnIter.hasNext()) {
    			LockNode txn = txnIter.next();
    			if (txn.getTransactionId().equals(t)) {
    				// transaction t is already waiting for the lock
    				return false;
   				}		
   			}
    		// transaction t is not in the page Q
    		pageQ.addLast(new LockNode(t,p));
    		lockTable.put(p, pageQ);
    		return false;
   		}
    }
    // used to determine if a transaction t holds the lock for page p
    public boolean holdsLock(TransactionId t, PageId p) {
    	acquireLockManagerLock();
    	// has lock for Lock Manager
    	LinkedList<LockNode> pageQ = this.lockTable.get(p);
    	if (pageQ != null) {
	    	LockNode lockHolder = pageQ.getFirst();
	    	if (lockHolder.equals(t) && lockHolder.hasLock()) {
	    		// txn has lock
	    		return true;
	    	}
    	}
    	releaseLockManagerLock();
    	return false;
    }
    
    //method to make txn wait until it receives the lock for the page
    private void waitForLock(TransactionId t, PageId p) {
    	while (holdsLock(t,p) == false) {
    		try {
    			Thread.sleep(1);
    			} catch (InterruptedException ignored) {}
    	}
    }
    
    // method to release the lock for a particular page
    public void releasePageLock(TransactionId t, PageId p) {
    	acquireLockManagerLock();
    	// has lock for LockManager
    	LinkedList<LockNode> pageQ = lockTable.get(p);
    	
    	pageQ.removeFirst();
    	if (!pageQ.isEmpty()) {
    		// give lock to next txn in line
    		(pageQ.getFirst()).getLock();
    	} else {
    		// remove the pageQ from lockTable
    		// NOT SURE THAT THIS WILL WORK
    		// IT MAY REMOVE ALL PAGE QUEUES IN THE BUCKET
    		lockTable.remove(p);
    	}
    	releaseLockManagerLock();
    }
}
	
	
