package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 *
 * @Threadsafe
 */
public class Catalog {
	
	/**
     * A help class to facilitate organizing the information of each table
     */
    public static class Table {
    	
    	/**
         * The file where the table is stored
         */
    	public final DbFile file;
    	
    	/**
         * The name of the table
         */
    	public final String name;
    	
    	/**
         * the primary key field for the table
         */
    	public final String pkeyField;
    	
    	/**
         * the table id for the table
         */
    	public final int tid;
    	
    	/**
         * The Table constructor
         */
    	public Table(DbFile file, String name, String pkeyField) {
    		this.tid = file.getId();
    		this.file = file;
            this.name = name;
            this.pkeyField = pkeyField;
        }
    }
    
    /**
     * Array of tables in Catalog
     */
    public Table[] tables;
    
    /**
     * Number of tables in Catalog
     */
    public int numTables;
    
    /**
     * Initial size of catalog (number of tables in tables array)
     * I only made this a variable so that if I change this size later in the project,
     * I wouldn't need to remember to update the clear() method to restore the Catalog 
     * to the new size
     */
    public final int initial_catalog_size = 5;
    
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        this.tables = new Table[this.initial_catalog_size]; 
        this.numTables = 0;
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     *
     * @param file      the contents of the table to add;  file.getId() is the identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name      the name of the table -- may be an empty string.  May not be null.  If a name
     * @param pkeyField the name of the primary key field
     *                  conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name, String pkeyField) {
    	if (name == null) {
    		throw new RuntimeException("Table name cannot be null");
    	}
    	//if tables array is full, resize it
    	if (this.numTables >= this.tables.length) {
    		Table[] temp = new Table[this.tables.length * 2];
    		System.arraycopy(this.tables, 0, temp, 0, this.numTables);
    		this.tables = temp;
    	}
    	//NOTE: THIS IS NOT OPTIMIZED OR SORTED IN ANY WAY!  (HeapFile Implementation)
    	this.tables[this.numTables] = new Table(file, name, pkeyField);
    	this.numTables += 1;
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     *
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *             this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
    	if (name == null) {
        	throw new NoSuchElementException("Table name must be a string, not null");
        }
        // iterates backwards through the tables array (starting with most recently added)
        // so that if there is a name conflict, it will return the table id for the most recently added table
        for (int i = this.numTables - 1; i >= 0; i--) {
        	if (this.tables[i].name.equals(name)) {
        		return this.tables[i].tid;
        	}
        }
        throw new NoSuchElementException("Table name is not in Catalog");
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
    	for (int i = this.numTables - 1; i >= 0; i--) {
        	if (this.tables[i].tid == tableid) {
        		return this.tables[i].file.getTupleDesc();
        	}
        }
        throw new NoSuchElementException("Table id is not in Catalog");
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
    	for (int i = this.numTables - 1; i >= 0; i--) {
        	if (this.tables[i].tid == tableid) {
        		return this.tables[i].file;
        	}
        }
        throw new NoSuchElementException("Table id is not in Catalog");
    }

    public String getPrimaryKey(int tableid) {
    	for (int i = this.numTables - 1; i >= 0; i--) {
        	if (this.tables[i].tid == tableid) {
        		return this.tables[i].pkeyField;
        	}
        }
    	throw new RuntimeException("Table id is not in Catalog");
    }

    public Iterator<Integer> tableIdIterator() {
        //Creates an Integer array of table ids, converts it to a list, then returns a iterator
    	Integer[] tableIds = new Integer[this.numTables];
        for (int i = 0; i < this.numTables; i++) {
        	tableIds[i] = this.tables[i].tid;
        }
        return Arrays.asList(tableIds).iterator();
    }

    public String getTableName(int id) {
    	for (int i = this.numTables - 1; i >= 0; i--) {
        	if (this.tables[i].tid == id) {
        		return this.tables[i].name;
        	}
        }
    	throw new RuntimeException("Table id is not in Catalog");
    }

    /**
     * Delete all tables from the catalog
     */
    public void clear() {
    	// restore tables array to a blank array of the original size
        this.tables = new Table[this.initial_catalog_size];
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     *
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t + (primaryKey.equals("")? "":(" key is " + primaryKey)));
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

