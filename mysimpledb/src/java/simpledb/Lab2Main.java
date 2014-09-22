package simpledb;
import java.io.*;
import java.util.NoSuchElementException;

public class Lab2Main {

    public static void main(String[] argv) {
    	// construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("some_data_file.dat"), descriptor);
        Database.getCatalog().addTable(table1, "test");
        BufferPool bp = Database.getBufferPool();
        
        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());

        try {
            // and run it
            f.open();
            
            Tuple new_t = null; 
            Field field = null;
            
            while (f.hasNext()) {
                Tuple tup = f.next();
                if (((IntField)tup.fields[1]).getValue() < 3){
                	// make a new tuple to replace tup
                	// the new tuple has all the same fields as tup except field1 is set to 3
                	new_t = new Tuple(descriptor);
                	for (int i = 0; i < descriptor.numFields(); i++) {
                		if (i != 1) {
                			field = tup.getField(i);
                		} else {
                				
                			//field = descriptor.getFieldType(i).parse(dis);
                			field = new IntField(3);
                		}
                		new_t.setField(i, field);
                	}
                	TransactionId tid2 = new TransactionId();
                	TransactionId tid3 = new TransactionId();
                	// field1 is < 3, so delete the tuple and replace it with same tuple except with field1 = 3
                	bp.deleteTuple(tid2, tup);
                	bp.insertTuple(tid3, table1.getId(), new_t);
                	Database.getBufferPool().transactionComplete(tid2);
                	Database.getBufferPool().transactionComplete(tid3);
                	
                }
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
            
            // Make new tuple with all fields set to 99
            new_t = new Tuple(descriptor);
            
            field = new IntField(99);
            for (int i = 0; i < descriptor.numFields(); i++) {
        		new_t.setField(i, field);
        	}
            //insert that tuple
            TransactionId tid4 = new TransactionId();
            bp.insertTuple(tid4, table1.getId(), new_t);
            bp.flushAllPages();
            // Print out all tuples
            TransactionId tid5 = new TransactionId();
            SeqScan f2 = new SeqScan(tid5, table1.getId());
            f2.open();
            while (f2.hasNext()) {
            	System.out.println(f2.next());
            }
            f2.close();
            Database.getBufferPool().transactionComplete(tid4);
            Database.getBufferPool().transactionComplete(tid5);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
    }

}