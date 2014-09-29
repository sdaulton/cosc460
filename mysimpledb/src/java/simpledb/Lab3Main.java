
package simpledb;

import java.io.IOException;

public class Lab3Main {

    public static void main(String[] argv) 
       throws DbException, TransactionAbortedException, IOException {

        System.out.println("Loading schema from file:");
        // file named college.schema must be in mysimpledb directory
        Database.getCatalog().loadSchema("college.schema");

        // SQL query: SELECT * FROM STUDENTS WHERE name="Alice"
        // algebra translation: select_{name="alice"}( Students )
        // query plan: a tree with the following structure
        // - a Filter operator is the root; filter keeps only those w/ name=Alice
        // - a SeqScan operator on Students at the child of root
        TransactionId tid = new TransactionId();
        SeqScan scanS = new SeqScan(tid, Database.getCatalog().getTableId("Students"));
        SeqScan scanT = new SeqScan(tid, Database.getCatalog().getTableId("Takes"));
        JoinPredicate p = new JoinPredicate(0, Predicate.Op.EQUALS, 0); //Students field 0 = S.sid, Takes field 0 = T.tid
        Join joinOp = new Join(p, scanS, scanT);

        // query execution: we open the iterator of the root and iterate through results
        System.out.println("Query results:");
        joinOp.open();
        while (joinOp.hasNext()) {
            Tuple tup = joinOp.next();
            System.out.println("\t"+tup);
        }
        joinOp.close();
        Database.getBufferPool().transactionComplete(tid);
    }

}

