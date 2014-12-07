package simpledb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Set;

/**
 * @author mhay
 */
class LogFileRecovery {

    private final RandomAccessFile readOnlyLog;

    /**
     * Helper class for LogFile during rollback and recovery.
     * This class given a read only view of the actual log file.
     *
     * If this class wants to modify the log, it should do something
     * like this:  Database.getLogFile().logAbort(tid);
     *
     * @param readOnlyLog a read only copy of the log file
     */
    public LogFileRecovery(RandomAccessFile readOnlyLog) {
        this.readOnlyLog = readOnlyLog;
    }

    /**
     * Print out a human readable representation of the log
     */
    public void print() throws IOException {
        // since we don't know when print will be called, we can save our current location in the file
        // and then jump back to it after printing
        Long currentOffset = readOnlyLog.getFilePointer();

        readOnlyLog.seek(0);
        long lastCheckpoint = readOnlyLog.readLong(); // ignore this
        System.out.println("BEGIN LOG FILE");
        while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    System.out.println("<T_" + tid + " BEGIN>");
                    break;
                case LogType.COMMIT_RECORD:
                    System.out.println("<T_" + tid + " COMMIT>");
                    break;
                case LogType.ABORT_RECORD:
                    System.out.println("<T_" + tid + " ABORT>");
                    break;
                case LogType.UPDATE_RECORD:
                    Page beforeImg = LogFile.readPageData(readOnlyLog);
                    Page afterImg = LogFile.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " UPDATE pid=" + beforeImg.getId() +">");
                    break;
                case LogType.CLR_RECORD:
                    afterImg = LogFile.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " CLR pid=" + afterImg.getId() +">");
                    break;
                case LogType.CHECKPOINT_RECORD:
                    int count = readOnlyLog.readInt();
                    Set<Long> tids = new HashSet<Long>();
                    for (int i = 0; i < count; i++) {
                        long nextTid = readOnlyLog.readLong();
                        tids.add(nextTid);
                    }
                    System.out.println("<T_" + tid + " CHECKPOINT " + tids + ">");
                    break;
                default:
                    throw new RuntimeException("Unexpected type!  Type = " + type);
            }
            long startOfRecord = readOnlyLog.readLong();   // ignored, only useful when going backwards thru log
        }
        System.out.println("END LOG FILE");

        // return the file pointer to its original position
        readOnlyLog.seek(currentOffset);

    }

    /**
     * Rollback the specified transaction, setting the state of any
     * of pages it updated to their pre-updated state.  To preserve
     * transaction semantics, this should not be called on
     * transactions that have already committed (though this may not
     * be enforced by this method.)
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     *
     * @param tidToRollback The transaction to rollback
     * @throws java.io.IOException if tidToRollback has already committed
     */
    public void rollback(TransactionId tidToRollback) throws IOException {
        print();
    	readOnlyLog.seek(0);
        long lastCheckPt = readOnlyLog.readLong();
        if (lastCheckPt == -1) {
        	// no checkpoint written in log
        	lastCheckPt = 0;
        }
    	readOnlyLog.seek(readOnlyLog.length() - 8); // undoing so move to end of logfile - 8 bytes to read the last long
        long logRecordPosition = readOnlyLog.readLong(); // read the start byte offset of the last record 
        int type = 0;
        long tid = 0;
        BufferPool bp = Database.getBufferPool();
        while (logRecordPosition >= 8) { // first record in log starts at byte offset 8
        	readOnlyLog.seek(logRecordPosition);
        	type = readOnlyLog.readInt();
        	
        	tid = readOnlyLog.readLong();
        	System.out.println("position " + logRecordPosition +"; tid: " +tid);
        	if (tid == tidToRollback.getId()) {
        		// this log record is for tidToRollback
        	 
	        	
	        	
	        	System.out.println("Type of record: " + type);
	        	switch (type) {
	        		case LogType.ABORT_RECORD:
	        			//do nothing
	        			break;
	        		case LogType.COMMIT_RECORD:
	        			// this shouldn't happen
	        			
	        			throw new IOException("Transaction " + tidToRollback.getId() + " has already committed");
	        		case LogType.UPDATE_RECORD:
	        			//NEED TO REPLACE PAGE IN BUFFER POOL AND FORCE FLUSH TO DISK
	        			// logCLR with page after and tid
	        			Page before = LogFile.readPageData(readOnlyLog);
	        			DbFile beforeFile = Database.getCatalog().getDatabaseFile(before.getId().getTableId());
	        			//overwrite page in DbFile with before image
	        			before.setBeforeImage();
	        			System.out.println("READING UPDATE RECORD for " + before.getId().toString());
	        			beforeFile.writePage(before);
	        			//discard copy of page in BufferPool, if there is one
	        			bp.discardPage(before.getId());
	        			//HOW DO I WRITE THE CLR RECORD
	        			// I NEED TO GET THE LOG FILE.  CAN I DO THIS?
	        			LogFile writeLog = Database.getLogFile();
	        			writeLog.logCLR(tidToRollback, before);
	        			break;
	        		case LogType.BEGIN_RECORD:
	        			// stop -- this is where the txn began  
	        			return;
	        		case LogType.CHECKPOINT_RECORD:
	        			// don't care
	        			break;
	        		case LogType.CLR_RECORD:
	        			// do nothing?
	        			break;
	        	}
        	}
        	logRecordPosition = movePosition(logRecordPosition); // move position to previous log record in log file
        	System.out.println("bottom position: " + logRecordPosition);
        }
        
    }
    
    //helper function to move position to previous log record in log file
    public long movePosition(long logRecordPosition) throws IOException {
    	// move to previous log record
		if (logRecordPosition - 8 <= 8) {
			logRecordPosition = -1;
		} else {
			readOnlyLog.seek(logRecordPosition - 8);
			logRecordPosition = readOnlyLog.readLong();
		}
    	return logRecordPosition;
    }

    /**
     * Recover the database system by ensuring that the updates of
     * committed transactions are installed and that the
     * updates of uncommitted transactions are not installed.
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     */
    public void recover() throws IOException {

        // some code goes here

    }
}
