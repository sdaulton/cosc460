package simpledb;

import java.util.ArrayList;
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
		public final TransactionId tid;
		public final PageId pid;
		public boolean typeIsX;
		
		//constructor
		public LockNode(TransactionId tid, PageId pid, boolean requestedX) {
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
	// storing txns waiting for a lock
	private HashMap<PageId, LockEntry> lockTable; 
	// maps to a linked list of all txns grants a lock on a page
	
	private LinkedList<TransactionId> abort_list;
	
	public LockManager(int numPages) {
		// make the size of the lock table equal to the number of pages in the bufferpool
		// hash map: maps pageId -> a LinkedList of LinkedLists where there is a linked list for each
		// unique PageId in the bucket.  The Linked list is the queue of nodes waiting
		// for the lock for that PageId.  The head of the LinkedList is the transaction
		// currently holding the node
		this.lockTable = new HashMap<PageId, LockEntry>(numPages);
		this.abort_list = new LinkedList<TransactionId>();
	}
    
	public synchronized void removePage(PageId pid) {
		lockTable.remove(pid);
		//DO I NEED TO ABORT TRANSACTIONS THAT ARE USING THIS PAGE?
	}
    
    // this is the only method called by a transaction to request the lock for a page
    public void acquirePageLock(TransactionId t, PageId p, boolean requestedX) throws TransactionAbortedException {
    	// get lock for LockManager
    	//acquireLockManagerLock();
    	boolean lockHeld = false;
    	synchronized(this) {
	    	// now has lock for lock manager
	    	lockHeld = requestLock(t,p, requestedX);
	    	//System.out.println("Thread " + t.getId() + "got Lock? " + lockHeld+ " Page " + p.pageNumber());
	    	// request has been entered
    	}
    	//releaseLockManagerLock();
    	if (!lockHeld) {
    		// waiting for lock
    		waitForLock(t,p, requestedX);
    	}
    	// txn has lock
    }
    
    // used by a txn to request a lock for a particular page
    // returns true if the txn now holds the lock for that page, and false if
    // is waiting for that lock
    private synchronized boolean  requestLock(TransactionId t, PageId p, boolean requestedX) throws TransactionAbortedException {
    	LockEntry lock = lockTable.get(p);
    	boolean gotLock = false;
    	if (abort_list != null) {
    		if (abort_list.contains(t)) {
    			abort_list.remove(t);
    			throw new TransactionAbortedException();
    		}
    	}
    	if (lock == null) {
    		//System.out.println("at 0 Thread " +t.getId() + " Page " + p.toString() + "requestedX" + requestedX);
    		lock = new LockEntry(p);
    		lock.grantedT.addLast(t);
    		lock.typeIsX = requestedX;
    		lockTable.put(p, lock);
    		return true;
    	}
    	LockNode txn = new LockNode(t, p, requestedX);
    	if (lock.grantedT.contains(t)){
    		//txn has a lock
    		if (lock.typeIsX) {
				//already has exclusive lock
    			//System.out.println("at 1 Thread " +t.getId() + " Page " + p.toString() + "requestedX" + requestedX);
    			lockTable.put(p, lock);
				return true;
			} else if (requestedX) {
				//has shared, but requested exclusive --> UPGRADE
				if (lock.grantedT.size() == 1) {
					// only this txn has a lock right now
					//System.out.println("at 2 Thread " +t.getId() + " Page " + p.toString() + "requestedX" + requestedX);
					lock.typeIsX = true;
					lockTable.put(p, lock);
					return true;
				} else {
					// other transactions also have locks currently
					//System.out.println("at 3 Thread " +t.getId() + " Page " + p.toString() + "requestedX" + requestedX);
					lock.waiting.addFirst(txn);
					lockTable.put(p, lock);
					return false;
				}
			} else {
				// has at least shared, requested shared
				//System.out.println("at 4 Thread " +t.getId() + " Page " + p.toString() + "requestedX" + requestedX);
				lockTable.put(p, lock);
				return true;
			}
    	} else if (!lock.waiting.isEmpty()) {
    		//check if this txn is already waiting for the lock
    		if (updateWaiting(txn, lock, false)) {
    			// txn is already waiting -- updates request type as well
    			//System.out.println("at 5 Thread " +t.getId() + " Page " + p.toString() + "requestedX" + requestedX);
    			return false;
    		} else {
    			//this txn is not waiting, not granted, and waiting is not empty
    			//System.out.println("at 6 Thread " +t.getId() + " Page " + p.toString() + "requestedX" + requestedX);
    			gotLock = false;
    		} 
		} else if (lock.grantedT.isEmpty()) {
    		// no txns waiting, lock is free
			//System.out.println("at 7 Thread " +t.getId() + " Page " + p.toString() + "requestedX" + requestedX);
			gotLock = true;
    	} else if(lock.waiting.isEmpty()) {
    		//another txn is holding the hold, but the waiting list is empty
    		if(!lock.typeIsX && !requestedX){
    			//System.out.println("at 8 Thread " +t.getId() + " Page " + p.toString() + "requestedX" + requestedX);
    			gotLock = true;
    		} else {
    			//System.out.println(lock.grantedT.getFirst());
    			// either the txn holding or txn requesting has a exclusive lock
    			//System.out.println("at 9 Thread " +t.getId() + " Page " + p.toString() + "requestedX" + requestedX);
    			gotLock = false;
    		}
    	}
    	
    	if (gotLock) {
    		lock.grantedT.addLast(t);
    		lock.typeIsX = requestedX;
    		lockTable.put(p, lock);
    		return true;
    	}
    	lock.waiting.addLast(txn);
    	lockTable.put(p, lock);
    	return false;
    	
    }
    
    
    public synchronized LockEntry updateLock(LockEntry lock) {
    	// give the lock to the next node
    	BufferPool bp = Database.getBufferPool();
    	LockNode node = lock.waiting.removeFirst();
    	//System.out.println("given to Thread "+ node.tid.getId());
    	lock.grantedT.add(node.tid);
    	synchronized (bp) {
    		if (bp.tid_time == null) {
    			//System.out.println("tid_time is null");
    		} else if (bp.tid_time.remove(node.tid) == null) {
    			//System.out.println("tid_time doesnt contain node.tid");
    		}
    		if (bp.tid_time.containsKey(node.tid)) {
    			bp.tid_time.put(node.tid, (bp.tid_time.remove(node.tid) + bp.TIMEOUT));
    		}
    		}
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
    				synchronized (bp) {
    		    		bp.tid_time.put(node.tid, bp.tid_time.remove(node.tid) + bp.TIMEOUT);
    		    	}
    				//System.out.println("and given to Thread "+ node.tid.getId());
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
    	//acquireLockManagerLock();
    	boolean hasLock = false;
    	synchronized (this) {
	    	// has lock for Lock Manager
	    	LockEntry lock = lockTable.get(p);
	    	if (lock != null) {
		    	if ((lock.grantedT.contains(t)) && ((lock.typeIsX) || (checkX == lock.typeIsX))) {
		    		hasLock = true;
		    		//System.out.println("Thread "+ t.getId() + "got lock from waitlist for page " + p.toString());
		    	}
	    	}
    	}
    	//releaseLockManagerLock();
		return hasLock;
    }
    
    //method to make txn wait until it receives the lock for the page
    private void waitForLock(TransactionId t, PageId p, boolean requestedX) throws TransactionAbortedException {
    	BufferPool bp = Database.getBufferPool();
    	
    	while (holdsLock(t,p, requestedX) == false) {
    		synchronized(bp) {
        		if (System.currentTimeMillis() > bp.tid_time.get(t) + bp.TIMEOUT) {
        			//System.out.println("aborted Thread " +t.getId());
        			throw new TransactionAbortedException();
        		}
        	}
    		//System.out.println("waiting"+ "Thread "+ t.getId());
    		
    		//acquireLockManagerLock();
    		
    		/*synchronized(this) {
    			if(abort_list != null && !abort_list.isEmpty()){
    	    		System.out.println(abort_list.size());
    	    		if (abort_list.size() != 0){
    	    			for (int i =0; i < abort_list.size(); i++){
    	    				System.out.println(abort_list.get(i).getId());
    	    			}
    	    			
    	    		}
    			}
	    		if (abort_list.contains(t)) {
	    			abort_list.remove(t);
	    			//releaseLockManagerLock();
	    			System.out.println("aborted "+ t.getId());
	    			throw new TransactionAbortedException();
	    			
	    		}
    		}*/
    		//releaseLockManagerLock();
    		try {
    			Thread.sleep(500);
    			//timeout++;
    			} catch (InterruptedException ignored) {}
    		
    		//if (timeout >=2) {
    			//System.out.println("aborted "+ t.getId());
    			//throw new TransactionAbortedException();
    			// abort all txns holding the lock
    			//System.out.println("aborted");
    			//acquireLockManagerLock();
    			//timeout = 0;
    			/*synchronized(this) {
	    			LockEntry lock = lockTable.get(p);
	    			while (!lock.grantedT.isEmpty()) {
	    				TransactionId tid = lock.grantedT.remove();
	    				abort_list.add(tid);
	    				//try {
	    					//Database.getBufferPool().transactionComplete(tid, false);
	    				//} catch (java.io.IOException e) {
	    					//throw new TransactionAbortedException();}
	    			}
	    			lockTable.put(p, lock);
    			}*/
    			//releaseLockManagerLock();
    		//}
    		//if(timeout == 6) {
    			//throw new TransactionAbortedException();
    		//}
    	}
    	/*synchronized(this) {
    		if (abort_list.contains(t)){
    			abort_list.remove(t);
    		}
    	}*/
    }
    
    // method to release the lock for a particular page
    public void releasePageLock(TransactionId t, PageId p) {
    	//acquireLockManagerLock();
    	synchronized(this) {
	    	// has lock for LockManager
	    	LockEntry lock = lockTable.get(p);
	    	lock.grantedT.remove(t);
	    	if (lock.grantedT.isEmpty()) {
	    		//System.out.println("granted is empty");
	    	}
	    	updateWaiting(new LockNode(t,p,false),lock, true);
	    	lock = lockTable.get(p);
	    	if (lock.grantedT.isEmpty() && (!lock.waiting.isEmpty())) {
	    		//System.out.println("giving to lock to next waiting");
	    		lock = updateLock(lock);
	    	}
	    	lockTable.put(p, lock);
    	}
    	//releaseLockManagerLock();
    }
    
   

    // checks if the LockNode is already waiting, if so, it updates the node to reflect 
    // the lock type requested and returns true, else returns false
    private synchronized boolean updateWaiting(LockNode txn, LockEntry lock, boolean abort) {
    	for (int i = 0; i < lock.waiting.size(); i++){
    		LockNode node = lock.waiting.get(i);
    		if (node.getTransactionId().equals(txn.getTransactionId())) {
    			if (!abort) {
					node.typeIsX = (node.typeIsX || txn.typeIsX);
					lock.waiting.add(i, node);
					lockTable.put(txn.pid, lock);
		    		return true;
    			} else {
    				//abort
    				lock.waiting.remove(node);
    				lockTable.put(txn.pid, lock);
    				return true;
    			}
    		}
    	}
    	return false;
    }
    
    
}	
	
