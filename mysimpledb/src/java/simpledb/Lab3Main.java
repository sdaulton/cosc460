
package simpledb;
import java.io.IOException;
import java.util.ArrayList;

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
        SeqScan scanP = new SeqScan(tid, Database.getCatalog().getTableId("Profs"));
        StringField hay = new StringField("hay", Type.STRING_LEN);
        Predicate p = new Predicate(1, Predicate.Op.EQUALS, hay);
        Filter filterProfs = new Filter(p, scanP);  // filter for professors with the name "hay"
        
        JoinPredicate jp1 = new JoinPredicate(1, Predicate.Op.EQUALS, 2); 
        JoinPredicate jp2 = new JoinPredicate(0, Predicate.Op.EQUALS, 0); 
        Join joinOp1 = new Join(jp1, scanT, filterProfs); //Join tuples from Takes T, filterProfs where T.cid = filterProfs.favoriteCourse
        Join joinOp2 = new Join(jp2, scanS, joinOp1); //Join tuples from Students S, joinOp1 where S.sid = joinOp1.sid
        // for projecting sName
        ArrayList<Integer> proj_indexes = new ArrayList<Integer>();
        proj_indexes.add(1);
        ArrayList<Type> proj_types = new ArrayList<Type>();
        proj_types.add(Type.STRING_TYPE);
        
        Project sName = new Project(proj_indexes, proj_types, joinOp2);  // project sName from joinOp2
        
        
        //query execution: we open the iterator of the root and iterate through results
        System.out.println("Query results:");
        sName.open();
        while (sName.hasNext()) {
            Tuple tup = sName.next();
            System.out.println("\t"+tup);
        }
        sName.close();
        Database.getBufferPool().transactionComplete(tid); 
        
    }

}

