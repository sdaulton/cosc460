package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

public class LockManager {
	
	static class LockEntry {
		private final PageId pid;
		private boolean typeIsX;
		private LinkedList<TransactionId> grantedT;
		private LinkedList<LockNode> waiting;
		
		private LockEntry(PageId pid) {
			this.pid = pid;
			this.grantedT = new LinkedList<TransactionId>();
			this.waiting = new LinkedList<LockNode>();
		}
	}
	
	// nodes in the wait list
	static class LockNode {
		private final TransactionId tid;
		private final PageId pid;
		public boolean typeIsX;
		
		//constructor
		private LockNode(TransactionId tid, PageId pid, boolean requestedX) {
			this.tid = tid;
			this.pid = pid;
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
	static private boolean inUse;
	// storing txns waiting for a lock
	private HashMap<PageId, LockEntry> lockTable; 
	// maps to a linked list of all txns grants a lock on a page
	
	public LockManager(int numPages) {
		this.inUse = false;
		// make the size of the lock table equal to the number of pages in the bufferpool
		// hash map: maps pageId -> a LinkedList of LinkedLists where there is a linked list for each
		// unique PageId in the bucket.  The Linked list is the queue of nodes waiting
		// for the lock for that PageId.  The head of the LinkedList is the transaction
		// currently holding the node
		this.lockTable = new HashMap<PageId, LockEntry>(numPages);
	}
    
	public void removePage(PageId pid) {
		acquireLockManagerLock();
		lockTable.remove(pid);
		//DO I NEED TO ABORT TRANSACTIONS THAT ARE USING THIS PAGE?
		releaseLockManagerLock();
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
    public void acquirePageLock(TransactionId t, PageId p, boolean requestedX) {
    	// get lock for LockManager
    	acquireLockManagerLock();
    	// now has lock for lock manager
    	boolean lockHeld = false;
    	lockHeld = requestLock(t,p, requestedX);
    	System.out.println(lockHeld + " "+ t.getId());
    	// request has been entered
    	releaseLockManagerLock();
    	if (!lockHeld) {
    		// waiting for lock
    		waitForLock(t,p, requestedX);
    	}
    	// txn has lock
    }
    
    // used by a txn to request a lock for a particular page
    // returns true if the txn now holds the lock for that page, and false if
    // is waiting for that lock
    private boolean requestLock(TransactionId t, PageId p, boolean requestedX) {
    	LockEntry lock = lockTable.get(p);
    	boolean gotLock = false;
    	if (lock == null) {
    		lock = new LockEntry(p);
    		lock.grantedT.addLast(t);
    		lock.typeIsX = requestedX;
    		lockTable.put(p, lock);
    		System.out.println("1");
    		return true;
    	}
    	
    	LockNode txn = new LockNode(t, p, requestedX);
    	if (lock.grantedT.contains(t)){
    		//txn has a lock
    		if (lock.typeIsX) {
				//already has exclusive lock
    			lockTable.put(p, lock);
    			System.out.println("2");
				return true;
			} else if (requestedX) {
				//has shared, but requested exclusive --> UPGRADE
				if (lock.grantedT.size() == 1) {
					// only this txn has a lock right now
					lock.typeIsX = true;
					lockTable.put(p, lock);
					System.out.println("3");
					return true;
				} else {
					// other transactions also have locks currently
					lock.waiting.addFirst(txn);
					lockTable.put(p, lock);
					System.out.println("4");
					return false;
				}
			} else {
				// has at least shared, requested shared
				lockTable.put(p, lock);
				System.out.println("5");
				return true;
			}
    	} else if (!lock.waiting.isEmpty()) {
    		//check if this txn is already waiting for the lock
    		if (updateWaiting(txn, lock)) {
    			// txn is already waiting -- updates request type as well
    			System.out.println("6");
    			return false;
    		} else {
    			//this txn is not waiting, not granted, and waiting is not empty
    			System.out.println("7");
    			gotLock = false;
    		} 
		} else if (lock.grantedT.isEmpty()) {
    		// no txns waiting, lock is free
			System.out.println("8");
    		gotLock = true;
    	} else if(lock.waiting.isEmpty()) {
    		//another txn is holding the hold, but the waiting list is empty
    		if(!lock.typeIsX && !requestedX){
    			gotLock = true;
    		} else {
    			// either the txn holding or txn requesting has a exclusive lock
    			gotLock = false;
    		}
    	}
    	
    	if (gotLock) {
    		lock.grantedT.addLast(t);
    		lock.typeIsX = requestedX;
    		lockTable.put(p, lock);
    		System.out.println("9");
    		return true;
    	}
    	lock.waiting.addLast(txn);
    	lockTable.put(p, lock);
    	System.out.println("10");
    	return false;
    	
    }
    
    
    public LockEntry updateLock(LockEntry lock) {
    	// give the lock to the next node
    	LockNode node = lock.waiting.removeFirst();
    	lock.grantedT.add(node.tid);
    	lock.typeIsX = node.typeIsX;
    	if (lock.typeIsX) {
    		//exclusive given
    		return lock;
    	} else {
    		//shared, check for other shared locks at top of waiting list
    		while (!lock.waiting.isEmpty()) {
    			node = lock.waiting.removeFirst();
    			if (!node.typeIsX) {
    				//shared
    				lock.grantedT.add(node.tid);
    			} else {
    				//exclusive, put back on waiting list
    				lock.waiting.addFirst(node);
    				return lock;
    			}
    			
    		}
    		return lock;
    	}
    }
    // NEED TO UPDATE WAIT FUNCTION TO DEAL WITH UPGRADES,
    // AND CHECKING FOR SHARED LOCK REQUESTS AT TOP OF WAIT LIST
    
    
    // used to determine if a transaction t holds the lock for page p
    public boolean holdsLock(TransactionId t, PageId p, boolean checkX) {
    	acquireLockManagerLock();
    	boolean hasLock = false;
    	// has lock for Lock Manager
    	LockEntry lock = lockTable.get(p);
    	if ((lock.grantedT.contains(t)) && ((lock.typeIsX) || (checkX == lock.typeIsX))) {
    		hasLock = true;
    	}
    	releaseLockManagerLock();
		return hasLock;
    }
    
    //method to make txn wait until it receives the lock for the page
    private void waitForLock(TransactionId t, PageId p, boolean requestedX) {
    	System.out.println(t.toString()+" waiting");
    	while (holdsLock(t,p, requestedX) == false) {
    		try {
    			Thread.sleep(1);
    			} catch (InterruptedException ignored) {}
    	}
    }
    
    // method to release the lock for a particular page
    public void releasePageLock(TransactionId t, PageId p) {
    	acquireLockManagerLock();
    	// has lock for LockManager
    	LockEntry lock = lockTable.get(p);
    	lock.grantedT.remove(t);
    	System.out.println(lock.grantedT.isEmpty());
    	if ((lock.typeIsX || lock.grantedT.isEmpty()) && (!lock.waiting.isEmpty())) {
    		//lock was exclusive
    		lock = updateLock(lock);
    	}
    	lockTable.put(p, lock);
    	releaseLockManagerLock();
    }
    
   

    // checks if the LockNode is already waiting, if so, it updates the node to reflect 
    // the lock type requested and returns true, else returns false
    private boolean updateWaiting(LockNode txn, LockEntry lock) {
    	LinkedList<LockNode> waiting = lock.waiting;
    	for (int i = 0; i < lock.waiting.size(); i++){
    		LockNode node = lock.waiting.get(i);
    		if (node.getTransactionId().equals(txn.getTransactionId())) {
				node.typeIsX = (node.typeIsX || txn.typeIsX);
				lock.waiting.add(i, node);
				lockTable.put(txn.pid, lock);
	    		return true;
    		}
    	}
    	return false;
    }
    
    
}	
	
