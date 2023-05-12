package M1;

import java.io.*;
import java.text.BreakIterator;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.management.ObjectName;

import M2.Methods2;
import M2.OctPoint;
import M2.Octree;

//import javax.lang.model.util.ElementScanner14;

import M2.SQLTerm;

public class DBApp {
    static NULL NULL = new NULL();
    public static int MaximumRowsCountinTablePage;
    public int MaximumEntriesinOctreeNode;
    private LinkedList<String> listofCreatedTables;
    private static long start, end;// for execution time calculation

    public DBApp() throws DBAppException {
        listofCreatedTables = new LinkedList<>();

        readCSVTablesIntoCreatedList();
        init();
    }

    public void init() {
        try {
			this.readConfig();
		} catch (DBAppException e) {
			e.printStackTrace();
		}
    };

    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws DBAppException {
        // surroud the whole method with try catch to catch any exception and re-throw it as DBAppException
        try
        {
        if(strTableName == null || strClusteringKeyColumn == null || htblColNameType == null || htblColNameMin == null || htblColNameMax == null)
            throw new DBAppException("One of the parameters is null!");
        if(strTableName.equals("") || strClusteringKeyColumn.equals("") || htblColNameType.isEmpty() || htblColNameMin.isEmpty() || htblColNameMax.isEmpty())
            throw new DBAppException("One of the parameters is empty!");
        //check if table already exists
        if(listofCreatedTables.contains(strTableName))
            throw new DBAppException("Table already exists! If you want to\n reset or Create it again," +
             " please call ." +
                    "DELETETableDependencies(TableName) method first then call createTable() method");

        validation(htblColNameType, htblColNameMin, htblColNameMax); // validation method checks if the type of table's columns is one of the 4 types specified in the description,validation also checks if any column does not have maxVal or minVal , hence throws exception


        for(String colName : htblColNameMin.keySet()) // converts all min and max column values to lowercase (hosain)
            htblColNameMin.put(colName, htblColNameMin.get(colName).toLowerCase());
        for(String colName : htblColNameMax.keySet())
            htblColNameMax.put(colName, htblColNameMax.get(colName).toLowerCase());

        readConfig();


        if(!htblColNameType.containsKey(strClusteringKeyColumn))
            throw new DBAppException("Clustering key column does not exist in the table!");

        // create the table
        Table tblCreated = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin,
                htblColNameMax, MaximumRowsCountinTablePage);
        listofCreatedTables.add(strTableName);

        String strTablePath = "src/resources/tables/" + strTableName;

        File newFile = new File(strTablePath);
        newFile.mkdir();
        newFile = new File(strTablePath + "/pages");
        newFile.mkdir();

        // making the Indicies folder (hosain)
        new File(strTablePath + "/Indicies").mkdir();

        // serialization
        serialize(strTablePath + "/" + strTableName + ".ser", tblCreated);
        OperationSignatureDisplay(tblCreated, htblColNameType,OpType.CREATE);
        List<String> dataline = new ArrayList<>();
        String a =  "false";
        Enumeration<String> strkey = htblColNameType.keys();
//        Enumeration<String> strelem = htblColNameType.elements();
//        Enumeration<String> strmin = htblColNameMin.elements();
//        Enumeration<String> strmax = htblColNameMax.elements();

//        int i = 0;
        while(strkey.hasMoreElements()) {
//        	System.out.println(strkey.nextElement());
        	String s = strkey.nextElement();
        	if(s.compareTo(strClusteringKeyColumn)==0)
        		a = "true";
        	dataline.add(strTableName);
        	dataline.add(s);
        	dataline.add(htblColNameType.get(s));
        	dataline.add(a);
        	dataline.add(null);
        	dataline.add(null);
        	dataline.add(htblColNameMin.get(s));
        	dataline.add(htblColNameMax.get(s));
        	intoMeta("MetaData.csv", dataline);
        	dataline.clear();
        	a = "false";
//        	i++;

           }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new DBAppException(e.getMessage());
        }

    }
    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)throws DBAppException {
        // surroud the whole method with try catch to catch any exception and re-throw it as DBAppException
        try
        {
        if(strTableName == null || htblColNameValue == null)
            throw new DBAppException("One of the parameters is null!");
        if (!listofCreatedTables.contains(strTableName))
            throw new DBAppException("You cannot insert into a table that has not been created yet");
        Methods.check_strings_are_Alphabitical(htblColNameValue);
        Methods.convert_strings_To_Lower_Case(htblColNameValue); // convert all string records in hashtable to lower case
        // 1- fetch the table from the disk
        String path = "src/resources/tables/" + strTableName + "/" + strTableName + ".ser";
        Table tblToInsertInto = (Table) deserialize(path);

        // 2- insert a record into it
        Vector<Object> vecValues = new Vector<>();
        if(htblColNameValue.get(tblToInsertInto.getStrClusteringKeyColumn())==null)
        	throw new DBAppException("Primary key can not be null");
        Enumeration<String> strEnumeration = htblColNameValue.keys();
        while (strEnumeration.hasMoreElements()) {
            String strColName = strEnumeration.nextElement();
//            tblToInsertInto.validateColType(colType(strColName));
            Column c = tblToInsertInto.getColumn(strColName);

            if(!colType(strColName,strTableName).equals(c.getStrColType()))
                throw new DBAppException();
            // if this Column does not exist , throw exception
            if (/*!tblToInsertInto.getVecColumns().contains(c)*/ c == null)
                throw new DBAppException("No such column. Check your Spelling for Typos!");
            // get the value
            Object strColValue = htblColNameValue.get(strColName);
            // check the value type
            try {
                tblToInsertInto.validateValueType(c, strColValue, colType(strColName, strTableName));

                // if it is the value of the clustering key, insert into the first cell
                if (strColName.equals(tblToInsertInto.getStrClusteringKeyColumn())) // if(c.isPrimary())
                    vecValues.add(0, strColValue);
                else
                    vecValues.add(strColValue);
            } catch (DBAppException dbe) {
                dbe.printStackTrace();
                throw new DBAppException(dbe.getMessage());
            }
        }

        // insert the non given column values with NULL (other than the clustering key, if any)
        if(vecValues.size() < tblToInsertInto.getVecColumns().size()) { //not all columns are inserted
            for (Column c : tblToInsertInto.getVecColumns()) {
                String colName = c.getStrColName();
                if (!htblColNameValue.containsKey(colName))
                    vecValues.add(tblToInsertInto.getColumnEquivalentIndex(colName), NULL); // insert null in the correct index of the column
            }
        }

        Row entry = new Row(vecValues);
        tblToInsertInto.insertAnEntry(entry);
        // 3-return table back to disk with the the new inserted value
        serialize(path, tblToInsertInto);
        OperationSignatureDisplay(OpType.INSERT, tblToInsertInto, htblColNameValue);
        }
        catch(Exception e)
        {
           	e.printStackTrace();
            throw new DBAppException(e.getMessage());
        }

    }
    public void updateTable(String strTableName, String strClusteringKeyValue,Hashtable<String, Object> htblColNameValue) throws DBAppException {
        // surround the whole method with try catch to catch any exception and re-throw it as DBAppException
        try
        {
        if(strClusteringKeyValue==null)
           	throw new DBAppException("Clustering key can not be null");
        if(strTableName == null || htblColNameValue == null)
            throw new DBAppException("One of the parameters is null!");
        if (!listofCreatedTables.contains(strTableName))
            throw new DBAppException("You cannot update a table that has not been created yet");
        Methods.check_strings_are_Alphabitical(htblColNameValue); // check if all string records inside the hashtable are alphabitical
        Methods.convert_strings_To_Lower_Case(htblColNameValue); // convert all string records in hashtable to lower case
        // 1- fetch the table from the disk
        String path = "src/resources/tables/" + strTableName + "/" + strTableName + ".ser";
        Table tblToUpdate = (Table) deserialize(path);

        Vector<Object> v = new Vector<>();
        Row rowToUpdate = new Row(v);

        // 2- Insert hashTable elements into vector
        for (String columnName : htblColNameValue.keySet()) {
//            tblToUpdate.validateColType(colType(columnName,strTableName));
            Object newValue = htblColNameValue.get(columnName);

            // if it is the value of the clustering key, insert into the first cell
            if (columnName.equals(tblToUpdate.getStrClusteringKeyColumn())) {
                //v.add(0, strClusteringKeyValue);
            	throw new DBAppException("You cannot update the clustering key");
            }
            else
                v.add(newValue);
        }

        // 3- Find the row to update using the clustering key value
        int candidateIdx = 0;
        if (strClusteringKeyValue != null) {
            String clustercolumn = tblToUpdate.getStrClusteringKeyColumn();
            Column c = tblToUpdate.getColumn(clustercolumn);
            Object objClusteringKeyVal = tblToUpdate.getValueBasedOnType(strClusteringKeyValue, c);
            candidateIdx = tblToUpdate.binarySrch(objClusteringKeyVal);
            Page candidatePage = tblToUpdate.loadPage(candidateIdx);

            int rowIdxToUpdate = tblToUpdate.findRowToUpdORdel(objClusteringKeyVal, candidateIdx);
            if (rowIdxToUpdate < 0) {
                System.out.println("No such row matches to update it");
                return;
            } else {
                //rowToUpdate.setData(v);
                candidatePage.updateRow(tblToUpdate,rowIdxToUpdate, htblColNameValue);
            }
            tblToUpdate.savePageToDisk(candidatePage, candidateIdx);
        }

//        else {
//            tblToUpdate.deleteRowsWithoutCKey(htblColNameValue);
//        }

        // 4- Update the values of the columns in the row
        //rowToUpdate.setData(v);

        // 5- return table & page back to disk after update
        serialize(path, tblToUpdate);
        OperationSignatureDisplay(OpType.UPDATE, tblToUpdate, htblColNameValue, strClusteringKeyValue);




        // test
        // Row entry = new Row(v);
        // rowtodelete.setData(v);
        // tblToUpdate.insertAnEntry(rowToUpdate);
        // System.out.println(rowtodelete.toString());
        // System.out.println(v.toString());
        // System.out.println(candidateIdx);
        // System.out.println(i);
        // test

        }
        catch(Exception e)
        {
            throw new DBAppException(e.getMessage());
        }

    }
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)throws DBAppException {
        // surroud the whole method with try catch to catch any exception and re-throw it as DBAppException
        try
        {
        if(strTableName == null || htblColNameValue == null)
            throw new DBAppException("One of the parameters is null!");
        if (!listofCreatedTables.contains(strTableName))
            throw new DBAppException("You cannot update a table that has not been created yet");
        Methods.convert_strings_To_Lower_Case(htblColNameValue); // convert all string records in hashtable to lower case
        // 1- fetch the table from the disk
        String path = "src/resources/tables/" + strTableName + "/" + strTableName + ".ser";
        Table tblToUpdate = (Table) deserialize(path);

        // 2- delete the row from the table (not yet deleted)
        Object clusteringKeyVal = null;

        Enumeration<String> strEnumeration = htblColNameValue.keys();
        while (strEnumeration.hasMoreElements()) {
            String strColName = strEnumeration.nextElement();
//            tblToUpdate.validateColType(colType(strColName));
            Column c = tblToUpdate.getColumn(strColName);
            // if this Column does not exist , throw exception
            if (!tblToUpdate.getVecColumns().contains(c))
                throw new DBAppException("No such column. Check your Spelling for typos!");
            // get the value
            Object strColValue = htblColNameValue.get(strColName);
            // check the value type
            tblToUpdate.validateValueType(c, strColValue, colType(strColName, strTableName));

            // if it is the value of the clustering key, insert into the first cell
            if (strColName.equals(tblToUpdate.getStrClusteringKeyColumn())) {
                clusteringKeyVal = strColValue;
            }
        }

        if (clusteringKeyVal != null) {
            int candidateIdx = tblToUpdate.binarySrch(clusteringKeyVal);
            Page candidatePage = tblToUpdate.loadPage(candidateIdx);

            int rowtodelete = tblToUpdate.findRowToUpdORdel(clusteringKeyVal, candidateIdx);
            if (rowtodelete < 0)
                System.out.println("No rows matches these conditions.");
            else
                candidatePage.deleteEntry(rowtodelete);

            tblToUpdate.savePageToDisk(candidatePage, candidateIdx);
        } else {
            tblToUpdate.deleteRowsWithoutCKey(htblColNameValue);
        }

        // 3- delete page if empty
        //int size = tblToUpdate.getVecPages().size();
        //Iterator<String> iteratePg = tblToUpdate.getVecPages().iterator();

        List<String> tempPages = new ArrayList<String>();
        for (int i = 0; i < tblToUpdate.getVecPages().size(); i++) {
            tempPages.add(tblToUpdate.getVecPages().get(i));
        }

        ListIterator<String> iteratetmp = tempPages.listIterator();
        int index = iteratetmp.nextIndex();

        while (iteratetmp.hasNext()) {
            String pagetodelete = (String) iteratetmp.next();
            if (tblToUpdate.loadPage(index).isEmpty()) {
                iteratetmp.remove();// deleted from temporary list

                tblToUpdate.getVecPages().remove(index);// deleted from original list

                //new File(this.getClass().getResource("/resources/tables/" + strTableName + "/pages/" + pagetodelete + ".ser").toURI()).delete();
                 // delete from disk
            }else index = iteratetmp.nextIndex();
        }

        // 4-return table back to disk after update
        OperationSignatureDisplay(OpType.DELETE, tblToUpdate, htblColNameValue);
        serialize(path, tblToUpdate);
        }
        catch(Exception e)
        {
            throw new DBAppException(e.getMessage());
        }

    }
    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,String[] strarrOperators)throws DBAppException{
        try
        {
            validateSQLTerms(arrSQLTerms);
            validateOperators(arrSQLTerms, strarrOperators);
            Set<Integer> candidatePages = new HashSet<Integer>();
            if(indexCanHelp_simplified(arrSQLTerms,strarrOperators))
                candidatePages = selectPages_UsingIndex_simplified(arrSQLTerms, strarrOperators);
            else
                candidatePages = selectPages_WithoutIndex(arrSQLTerms[0]._strTableName);
            Set<Row> matchingRows = selectMatchingRows(arrSQLTerms, strarrOperators, candidatePages);
            return matchingRows.iterator();
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new DBAppException(e.getMessage());
        }
    }
    public void createIndex(String strTableName,String[] strarrColName) throws DBAppException
    {
        try
        {
        validateTableExists(strTableName);
        validateColNames(strTableName,strarrColName);
        for(String colName : strarrColName)
        {
            validateColumnExistsInTable(colName, strTableName);
            validateColumnNotIndexed(colName, strTableName);
        }
        String indexName = "";
        for(String colName : strarrColName)
            indexName += colName;
        indexName += "Index";
            writeIndexInMetadata(strTableName, strarrColName, indexName);// replace "null" with the index name
        // next, create the index file
        String tablePath = "src/resources/tables/" + strTableName + "/" + strTableName + ".ser";
        Table table = (Table) deserialize(tablePath);
        Column x_dimention_column = table.getColumn(strarrColName[0]);
        Column y_dimention_column = table.getColumn(strarrColName[1]);
        Column z_dimention_column = table.getColumn(strarrColName[2]);
        Comparable min_x = x_dimention_column.getMinValue();
        Comparable min_y = y_dimention_column.getMinValue();
        Comparable min_z = z_dimention_column.getMinValue();
        Comparable max_x = x_dimention_column.getMaxValue();
        Comparable max_y = y_dimention_column.getMaxValue();
        Comparable max_z = z_dimention_column.getMaxValue();
        Octree tree = new Octree(new OctPoint(min_x, min_y, min_z), new OctPoint(max_x, max_y, max_z),this.MaximumEntriesinOctreeNode);
        fillTreeFromTable(tree, table, strarrColName);
        String treePath = "src/resources/tables/" + strTableName + "/Indicies/" + indexName + ".ser";
        serialize(treePath, tree);
        serialize(tablePath, table);
        System.out.println("index  " + indexName + "  created successfully!");
        }
        catch(Exception e)
        {
            e.printStackTrace();
            throw new DBAppException(e.getMessage());
        }
}
    // M1 helpers
    public void DELETETableDependencies(String strTableName) throws DBAppException {
        // delete its references in the csv files and listofCreatedTables

        // first, read all data and extract all tables other than the given table
        List<String> data = new ArrayList<>();
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader("MetaData.csv"));
            String line = br.readLine();

            while (line != null) {
                String[] attributes = line.split(",");
                if(!attributes[0].equals(strTableName))
                    data.add(line);
                line = br.readLine();
            }
            br.close();
        } catch ( IOException e) {
            throw new DBAppException("Error reading csv file");
        }

        // second, write the new data to the csv file
        try {
            File file = new File("MetaData.csv");
            new FileWriter(file, false).close();;
            FileWriter fr = new FileWriter(file, true);
            PrintWriter printWriter = new PrintWriter(fr);
            StringBuilder stringBuilder = new StringBuilder();
            for (String line : data) {
                stringBuilder.append(line);
                stringBuilder.append('\n');
            }
            printWriter.write(stringBuilder.toString());
            printWriter.flush();
            printWriter.close();
        } catch (IOException e) {
            throw new DBAppException(e.getMessage());
        }

        // third, delete the table from the list of created tables
        listofCreatedTables.remove(strTableName);
    }

    public static void intoMeta(String filePath,List<String> dataline) throws DBAppException
    {
        // first create file object for file placed at location
        // specified by filepath
        File file = new File(filePath);
        try {
            // create FileWriter object with file as parameter
//            FileWriter outputfile = new FileWriter(file);

            // create CSVWriter object filewriter object as parameter
//            BufferedWriter writer = new BufferedWriter(outputfile);
            CsvWriter writer = new CsvWriter(file);

             //adding header to csv
            if( !CsvWriter.isHeadersWriten()) {
	            String[] header = {"Table Name", "Column Name", "Column Type", "ClusteringKey", "IndexName","IndexType", "min", "max" };
	            List<String> hTokens = new ArrayList<>();
	            for (int i = 0; i < header.length; i++) {
	            	hTokens.add(header[i]);
				}
	            writer.writeHeaders(hTokens);
            }
//            BufferedReader br = new BufferedReader(new FileReader(filePath));
//            String line = br.readLine();
//    		int i =0;
//    		while (line != null) {
//    			String[] content = line.split(",");
//    			System.err.println(content[i]);
//    			i++;
//    			if(i==8) {
//    				i=0;
//    				line = br.readLine();
//    			}
//
//    		}
            writer.writeRow(dataline);

            // closing writer connection
            //writer.close();
        }
        catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }
    public static void OperationSignatureDisplay(OpType operation, Table table, Hashtable<String,Object> htblColNameVal) {
        switch(operation) {
            case INSERT:

                StringBuilder output = new StringBuilder("-------SQL Equivalent Command:\n"
                		+ "INSERT INTO " +table.getStrTableName() + "(");

                Enumeration<String> en = htblColNameVal.keys();
                String[] cols = new String[htblColNameVal.size()];
                int k = 0;
                while (en.hasMoreElements()) {
                	cols[k] = (String) en.nextElement();
                	output.append((k == cols.length-1)?cols[k++]+") VALUES (": cols[k++]+", ");
                }

                for (int i = 0; i < cols.length; i++) {
					output.append((i == cols.length-1)?htblColNameVal.getOrDefault(cols[i], "Null")+");"
													: htblColNameVal.getOrDefault(cols[i], "Null")+", ");
				}
                output.append("\n\nand Result Set Output Table: ####################\n")
                		.append(table.toString()).append("\n######################\n");

                System.out.println(output);

                break;
            case DELETE:

                StringBuilder output2 = new StringBuilder("-------SQL Equivalent Command:\n"
                		+ "DELETE FROM " +table.getStrTableName());
             if (htblColNameVal.size() > 0) {
                	output2.append(" WHERE ");

                Enumeration<String> en2 = htblColNameVal.keys();
                String[] cols2 = new String[htblColNameVal.size()];
                int k2 = 0;
                while (en2.hasMoreElements())	cols2[k2++] = (String) en2.nextElement();


                for (int i = 0; i < cols2.length; i++) {

                	output2.append(cols2[i]+" = ");
					output2.append(htblColNameVal.getOrDefault(cols2[i], "Null"));
					if( i < cols2.length-1) output2.append("AND ");
					else output2.append(";");
				}
            }
                output2.append("\n\nand Result Set output Table: ####################\n")
                		.append(table.toString()).append("\n######################\n");

                System.out.println(output2);

                break;


            default: System.out.println("No enough or Wrong Info Given");

        }
    }
    public static void OperationSignatureDisplay(Table table, Hashtable<String,String> htblColNameType, OpType operation){
        switch(operation){
            case CREATE:

                    StringBuilder output4 = new StringBuilder("-------SQL Equivalent Command:\n"
                    		+ "CREATE TABLE " +table.getStrTableName() + " (\n");

                    Enumeration<String> en4 = htblColNameType.keys();
                    String[] cols4 = new String[htblColNameType.size()];
                    int k4 = 0;
                    while (en4.hasMoreElements()) {
                    	cols4[k4++] = (String) en4.nextElement();
                    }

                    for (int i = 0; i < cols4.length; i++) {
                       	output4.append("\t" + cols4[i]+" :: ");
                       	if(!table.getStrClusteringKeyColumn().equals(cols4[i]))
                       		output4.append((i == cols4.length-1)?htblColNameType.getOrDefault(cols4[i], "Null")+"\n );\n\n"
       													: htblColNameType.getOrDefault(cols4[i], "Null")+",\n");
                       	else
                       		output4.append((i == cols4.length-1)?htblColNameType.getOrDefault(cols4[i], "Null")+"  Primary Key\n );\n\n"
       													: htblColNameType.getOrDefault(cols4[i], "Null")+"  Primary Key,\n");
                    }
                    System.out.println(output4);
                    break;
            default: System.out.println("No enough or Wrong Info Given");

        }

    }
    public static void OperationSignatureDisplay(OpType operation, Table table, Hashtable<String,Object> htblColNameVal, String clusterKey) {
	    if(operation == OpType.UPDATE) {
	        StringBuilder output3 = new StringBuilder("-------SQL Equivalent Command:\n"
	        		+ "UPDATE " +table.getStrTableName()+ " SET\n");

	        Enumeration<String> en3 = htblColNameVal.keys();
	        String[] cols3 = new String[htblColNameVal.size()];
	        int k3 = 0;
	        while (en3.hasMoreElements())	cols3[k3++] = (String) en3.nextElement();


	        for (int i = 0; i < cols3.length; i++) {

	        	output3.append("\t"+cols3[i]+" = ");
				output3.append(htblColNameVal.getOrDefault(cols3[i], "Null"));
				if( i < cols3.length-1) output3.append(",\n");
				else output3.append("\nWHERE " + table.getStrClusteringKeyColumn() + " = " + clusterKey+";");
			}
	        output3.append("\n\nand Result Set output Table: ####################\n")
	        		.append(table.toString()).append("\n######################\n");

	        System.out.println(output3);

	    }else	System.out.println("No enough or Wrong Info Given");
    }
    public void readConfig() throws DBAppException {
        /*
         * this method objective is to read the DBApp configuration file in order to
         * extract data needed
         * for setting the instance variables of the DBApp Class
         * (MaximumRowsCountinTablePage,MaximumEntriesinOctreeNode)
         */
        String filePath = "src/resources/DBApp.config";

        try {
            FileReader fr = new FileReader(filePath); // throws FileNotFoundException
            BufferedReader br = new BufferedReader(fr);
            String readingOut1stLine = br.readLine(); // throws IOException
            // max rows in a table page
            StringTokenizer strTokenizer = new StringTokenizer(readingOut1stLine);
            while (strTokenizer.hasMoreElements()) {
                String tmp = strTokenizer.nextToken();
                if (tmp.equals("="))
                    MaximumRowsCountinTablePage = Integer.parseInt(strTokenizer.nextToken());
            }

            // max entries in OctTree
            String readingOut2ndLine = br.readLine();
            strTokenizer = new StringTokenizer(readingOut2ndLine);
            while (strTokenizer.hasMoreElements()) {
                String tmp = strTokenizer.nextToken();
                if (tmp.equals("="))
                    MaximumEntriesinOctreeNode = Integer.parseInt(strTokenizer.nextToken());
            }

            br.close();
        } catch (FileNotFoundException fnfe) {
            throw new DBAppException("Configuration file cannot be found");
        } catch (IOException ioe) {
            throw new DBAppException(ioe.getMessage());
        }
    }
    public static void starty() {
        if (start == 0)
            start = System.currentTimeMillis();
    }
    public static void endy() {
        end = System.currentTimeMillis();
        System.out.println('\n' + "exited with execution time: " /* +start+" " +start+" " */
                + ((float) (end - start)) / 1000 + "_seconds");
    }
    public static void serialize(String path, Serializable obj) {
        try {
            FileOutputStream fileOutStr = new FileOutputStream(path);
            ObjectOutputStream oos = new ObjectOutputStream(fileOutStr);
            oos.writeObject(obj);
            oos.close();
            fileOutStr.close();
        } catch (IOException i) {
            i.printStackTrace();

        }
    }
    public static Object deserialize(String path) throws DBAppException {
        Object obj = null;
        try {
            FileInputStream fileInStr = new FileInputStream(path);
            ObjectInputStream ois = new ObjectInputStream(fileInStr);
            obj = ois.readObject();
            ois.close();
            fileInStr.close();
        } catch (IOException | ClassNotFoundException i) {
            throw new DBAppException(path + " not found");
        }
        return obj;
    }
    public void readCSVTablesIntoCreatedList() throws DBAppException {
        BufferedReader br;
		try {
            br = new BufferedReader(new FileReader("MetaData.csv"));
            String line ;
            br.readLine();
            line = br.readLine();
            HashSet <String> set = new HashSet<>();
            while (line != null) {
                String[] values = line.split(",");
                if(!set.contains(values[0]))    listofCreatedTables.add(values[0]);
                set.add(values[0]);
                line = br.readLine();
            }

            br.close();
        } catch ( IOException e2) {
            throw new DBAppException("Error reading csv file");
        }

	}
    public static String colType(String colName, String tableName) throws DBAppException {
        String s = null;
        try {
        BufferedReader br = new BufferedReader(new FileReader("MetaData.csv"));
        String line = br.readLine();
        while (line != null) {
            String[] content = line.split(",");
            if (tableName.equals(content[0]) && content[1].compareTo(colName)==0) {
                s = content[2];
            }
            line = br.readLine();
        }
        br.close();
        } catch (IOException e) {
        	throw new DBAppException(e.getMessage());
		}

        return s;
    }

    ///////////////////////////////////////////// below are Helpers for M2

    // createIndex helpers
    private static void writeIndexInMetadata(String tableName, String[] columnNames, String indexName) throws IOException {// puts the index name in the metadata file instead of null in each column in columnNames

        BufferedReader reader = new BufferedReader(new FileReader("metadata.csv"));
        BufferedWriter writer = new BufferedWriter(new FileWriter("temp.csv"));

        String line;
        while ((line = reader.readLine()) != null) {
            String[] row = line.split(",");
            if (row[0].equals(tableName)) {
                for (int i = 0; i < columnNames.length; i++) {
                    if (row[1].equals(columnNames[i])) {
                        row[4] = indexName;
                        row[5] = "Octree";
                        break;
                    }
                }
            }
            writer.write(String.join(",", row));
            writer.newLine();
        }

        reader.close();
        writer.close();

        // Replace the original metadata file with the updated version
        new File("metadata.csv").delete();
        new File("temp.csv").renameTo(new File("metadata.csv"));
    }
    private void fillTreeFromTable(Octree tree, Table table, String[] strarrColName) throws DBAppException, IOException {
        for (String strPageID : table.getVecPages())
        {
            String pagePath = "src/resources/tables/" + table.getStrTableName() + "/pages/" + strPageID + ".ser";                                                                   // page10.ser
            Page page = (Page) deserialize(pagePath);
            for (Row row : page.getData())
            {
                Comparable row_x_value = (Comparable) row.getColumnValue(strarrColName[0],table.getStrTableName());
                Comparable row_y_value = (Comparable) row.getColumnValue(strarrColName[1],table.getStrTableName());
                Comparable row_z_value = (Comparable) row.getColumnValue(strarrColName[2],table.getStrTableName());
                int pageID = Integer.parseInt(strPageID.substring(4));
                tree.insertPageIntoTree(row_x_value, row_y_value, row_z_value,pageID);
            }
            serialize(pagePath, page);

        }


    }

    // select helpers
    private Set<Integer> selectPages_WithoutIndex(String tableName) {
        return getTablePagesIDs(tableName);

    }
    private Set<Row> selectMatchingRows(SQLTerm[] arrSQLTerms, String[] strarrOperators, Set<Integer> candidatePages) throws DBAppException, IOException {
    Set<Row>[] separated_SQLTermsResults = new Set[arrSQLTerms.length];
    for(int i = 0 ; i < separated_SQLTermsResults.length ; i++)
        separated_SQLTermsResults[i] = new HashSet<Row>();
    for (Integer page_id : candidatePages) 
    {
        String pagePath = "src/resources/tables/" + arrSQLTerms[0]._strTableName + "/pages/page" + page_id + ".ser";
        Page page = (Page) deserialize(pagePath);
        for(int i = 0 ; i < separated_SQLTermsResults.length ; i++)
            separated_SQLTermsResults[i].addAll(singleSQLTermResult(arrSQLTerms[i], page));
        serialize(pagePath, page);
    }
    // now we have all the rows that match each SQLTerm in separated_SQLTermsResults
    // we need to apply the operators on them
    return applyOperatorsFromLeftToRight(separated_SQLTermsResults, strarrOperators);
    }

    // misc helpers
    public Set<String> getAllTableIndicies(String tableName) throws IOException { 
        Set<String> indicies = new HashSet<>();
        BufferedReader br = new BufferedReader(new FileReader("MetaData.csv"));
        String line = br.readLine();
        while (line != null)
        {
            String[] content = line.split(",");
            if (tableName.equals(content[0]) && !content[4].equals("null"))
                indicies.add(content[4]);
            line = br.readLine();
        }
        br.close();
        return indicies;
    }  
    public boolean termsFormIndex(SQLTerm term1, SQLTerm term2, SQLTerm term3, String tableName) throws IOException{
        for(String indexName : getAllTableIndicies(tableName))
            for(String posiibleIndexName : Methods2.getAllPermutations(term1._strColumnName, term2._strColumnName, term3._strColumnName))
                if(indexName.equals(posiibleIndexName + "Index"))
                   return true;
        return false;        
    }    
    public String getColAxisInIndex(String indexName, String colName){

        String indexColumns = indexName.substring(0, indexName.length()-5); // remove "Index" from indexName
        if(colName.equals(indexColumns.substring(0,colName.length())))
            return "x";
        if(colName.equals(indexColumns.substring(indexName.length()-colName.length(), indexName.length())))
            return "z";
        return "y";
    }

    // selectMatchingRows helpers
    private Set<Row> singleSQLTermResult(SQLTerm sqlTerm, Page page) throws IOException, DBAppException { 
        Set<Row> result = new HashSet<Row>();
        Comparable sqlTermValue = (Comparable) sqlTerm._objValue;
        for(Row row : page.getData())
        {
            Comparable cellValue = (Comparable) row.getColumnValue(sqlTerm._strColumnName, sqlTerm._strTableName);
            switch(sqlTerm._strOperator)
            {
                case "=" : if(cellValue.compareTo(sqlTermValue) == 0) result.add(row); break;
                case "!=" : if(cellValue.compareTo(sqlTermValue) != 0) result.add(row); break;
                case ">" : if(cellValue.compareTo(sqlTermValue) > 0) result.add(row); break;
                case ">=" : if(cellValue.compareTo(sqlTermValue) >= 0) result.add(row); break;
                case "<" : if(cellValue.compareTo(sqlTermValue) < 0) result.add(row); break;
                case "<=" : if(cellValue.compareTo(sqlTermValue) <= 0) result.add(row); break;
            }
        }
        return result;
    }
    private <T> Set<T> applyOperatorsFromLeftToRight(Set<T>[] separated_SQLTermsResults, String[] strarrOperators) { // to do
        Stack<Set<T>> dataStack = new Stack<>();
        Stack<String> operatorsStack = new Stack<String>();
        for(int i = separated_SQLTermsResults.length - 1; i >= 0; i--)
            dataStack.push(separated_SQLTermsResults[i]);
        for(int i = strarrOperators.length - 1; i >= 0; i--)
            operatorsStack.push(strarrOperators[i]);
        while(!operatorsStack.isEmpty())
            dataStack.push(applySingleOperator(dataStack.pop(), dataStack.pop(), operatorsStack.pop()));
        return dataStack.pop();
    }
    private <T> Set<T> applySingleOperator(Set<T> set1, Set<T> set2, String operand) { 
        Set<T> result = new HashSet<>(set1.size() + set2.size());
        if(operand.equals("AND"))
        {
            for(T obj : set1)
                if(set2.contains(obj))
                    result.add(obj);
        }
        else if(operand.equals("OR"))
        {
            for(T obj : set1)
                result.add(obj);
            for(T obj : set2)
                if(!set1.contains(obj))
                    result.add(obj);
        }
        else if(operand.equals("XOR"))
        {
            for(T obj : set1)
                if(!set2.contains(obj))
                    result.add(obj);
            for(T obj : set2)
                if(!set1.contains(obj))
                    result.add(obj);
        }

        return result;
    }

    // misc validations
    private static void validation(Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
                                   Hashtable<String, String> htblColNameMax) throws DBAppException {

        Enumeration<String> strEnumeration = htblColNameType.keys();
        while (strEnumeration.hasMoreElements()) {
            String strColName = strEnumeration.nextElement();
            String strColType = htblColNameType.get(strColName);

            if (!(strColType.equals("java.lang.Integer") || strColType.equals("java.lang.String") ||
                    strColType.equals("java.lang.Double") || strColType.equals("java.util.Date")))
                throw new DBAppException("The type " + strColType + " is not supported");

            if (!htblColNameMin.containsKey(strColName) || !htblColNameMax.containsKey(strColName)) {
                throw new DBAppException("The column " + strColName + " must exist");

            }

        }
        if (!( htblColNameType.size() == htblColNameMin.size() && htblColNameType.size() == htblColNameMax.size()))
            throw new DBAppException("Columns not matching");


    }
    private void validateTableExists(String strTableName) throws DBAppException, IOException {
        // search for table name from the metadata file
        // if not found throw exception
        boolean found = false;
        BufferedReader br = new BufferedReader(new FileReader("MetaData.csv"));
        String line = br.readLine();
        while (line != null)
        {
            String[] content = line.split(",");
            if (strTableName.equals(content[0]))
            {
                found = true;
                break;
            }
            line = br.readLine();
        }
        br.close();

        if (!found)
            throw new DBAppException("Table " + strTableName + " does not exist !");


    }
    private void validateColumnExistsInTable(String strColName, String strTableName) throws DBAppException, IOException {
        validateTableExists(strTableName);
        boolean found = false;
        BufferedReader br = new BufferedReader(new FileReader("MetaData.csv"));
        String line = br.readLine();
        while (line != null)
        {
            String[] content = line.split(",");
            if (strTableName.equals(content[0]) && strColName.equals(content[1]))
            {
                found = true;
                break;
            }
            line = br.readLine();
        }
        br.close();

        if (!found)
            throw new DBAppException("Column " + strColName + " does not exist in table " + strTableName + " !");
    }
    private void validateColumnNotIndexed(String strColName, String strTableName) throws IOException, DBAppException{
        boolean found = false;
        BufferedReader br = new BufferedReader(new FileReader("MetaData.csv"));
        String line = br.readLine();
        while (line != null)
        {
            String[] content = line.split(",");
            if (strTableName.equals(content[0]) && strColName.equals(content[1]) && !content[4].equals("null"))
            {
                found = true;
                break;
            }
            line = br.readLine();
        }
        br.close();

        if (found)
            throw new DBAppException("Column " + strColName + " in table " + strTableName + " is already indexed !");

    }
    private void validateSQLTerms(SQLTerm[] arrSQLTerms) throws DBAppException, IOException {
        String tableName = arrSQLTerms[0]._strTableName;
        validateTableExists(tableName);
        for(SQLTerm term : arrSQLTerms)
        {
            String currentTable = term._strTableName;
            if(!currentTable.equals(tableName))
                throw new DBAppException("All SQL terms must be on the same table !");
            String colName = term._strColumnName;
            validateColumnExistsInTable(colName, currentTable);
            String operator = term._strOperator;
            boolean operatorIsSupported = operator.equals("=") || operator.equals("!=" ) || operator.equals(">") || operator.equals("<") || operator.equals(">=") || operator.equals("<=");
            if(!operatorIsSupported)
                throw new DBAppException("Operator " + operator + " is not supported !");
            String colType = colType(colName, currentTable);
            if(colType == null)
                throw new DBAppException("Column " + colName + " does not exist in table " + currentTable + " !");
            if(! term._objValue.getClass().getName().equals(colType))
                throw new DBAppException("Value " + term._objValue + " is not of type " + colType + " !");
       }
    }
    private void validateOperators(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        if(strarrOperators.length != arrSQLTerms.length - 1)
            throw new DBAppException("Number of operators does not match number of terms !");
        for(String operator : strarrOperators)
            if(!(operator.equals("AND") || operator.equals("OR") || operator.equals("XOR")))
                throw new DBAppException("Operator " + operator + " is not supported !");


    }
    private void validateColNames(String strTableName, String[] strarrColName) throws DBAppException, IOException {
        if(strarrColName.length != 3)
            throw new DBAppException("you must specify exactly 3 column names to be used in the index!");
        for(String colName : strarrColName)
            validateColumnExistsInTable(colName, strTableName);



    }

    public Set<Integer> getTablePagesIDs(String tableName)
    {
        File directory = new File("src/resources/tables/" + tableName + "/pages");
        File[] files = directory.listFiles();
        HashSet<Integer> pages_ids = new HashSet<Integer>();
        for (File file : files) 
        {
            String fileName = file.getName(); // page10.ser for example
            pages_ids.add(Integer.parseInt(fileName.substring(4, fileName.length()-4))); // to get the 10 only from page10.ser

        }
        return pages_ids;   
    }
    public boolean indexCanHelp_simplified(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException, IOException {

        String tableName = arrSQLTerms[0]._strTableName;
        for(int i = 0; i < arrSQLTerms.length-2; i++)
            if(termsFormIndex(arrSQLTerms[i] ,arrSQLTerms[i+1] , arrSQLTerms[i+2] , tableName))
                if(strarrOperators[i].equals("AND") && strarrOperators[i+1].equals("AND"))
                    return true;
        return false;
    }
    public Set<Integer> selectPages_UsingIndex_simplified(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws IOException, DBAppException    
    {
        Set<Integer> allTablePages = getTablePagesIDs(arrSQLTerms[0]._strTableName);
        String tableName = arrSQLTerms[0]._strTableName;
        List<Set<Integer>> resultPages = new LinkedList<Set<Integer>>();
        for(int i = 0; i < arrSQLTerms.length-2; i++)
        {
            if(termsFormIndex(arrSQLTerms[i] ,arrSQLTerms[i+1] , arrSQLTerms[i+2] , tableName))
            {
                if(strarrOperators[i].equals("AND") && strarrOperators[i+1].equals("AND"))
                {
                    resultPages.add(getPagesFromIndex(arrSQLTerms[i], arrSQLTerms[i+1], arrSQLTerms[i+2], tableName));
                    i+=2;
                }
            }
            else
            {
                resultPages.add(allTablePages);
            }

        }

        resultPages.add(allTablePages); // because we didn't add corrrosponding pages for the last 2 terms
        resultPages.add(allTablePages);
        Set<Integer>[] resultPagesArray = resultPages.toArray(new Set[resultPages.size()]);
        return applyOperatorsFromLeftToRight(resultPagesArray, strarrOperators);
    }             
    private Set<Integer> getPagesFromIndex(SQLTerm sqlTerm1, SQLTerm sqlTerm2, SQLTerm sqlTerm3, String tableName) throws IOException, DBAppException {
       
        Set<String> allIndicies = getAllTableIndicies(tableName);
        String targetIndexName = "";
        boolean found = false;
        for(String currentIndex : allIndicies)
        {
            if(found)
                break;
            for(String possibleName : Methods2.getAllPermutations(sqlTerm1._strColumnName, sqlTerm2._strColumnName, sqlTerm3._strColumnName))
            {
                if(currentIndex.equals(possibleName))
                {
                    targetIndexName = currentIndex;
                    found = true;
                    break;
                }
            }
        }
        SQLTerm x_dimention_term = null;
        SQLTerm y_dimention_term = null;
        SQLTerm z_dimention_term = null;
        String term1_dimension = getColAxisInIndex(targetIndexName, sqlTerm1._strColumnName);
        String term2_dimension = getColAxisInIndex(targetIndexName, sqlTerm2._strColumnName);
        String term3_dimension = getColAxisInIndex(targetIndexName, sqlTerm3._strColumnName);

        if(term1_dimension.equals("x"))
            x_dimention_term = sqlTerm1;
        else if(term1_dimension.equals("y"))
            y_dimention_term = sqlTerm1;
        else if(term1_dimension.equals("z"))
            z_dimention_term = sqlTerm1;

        if(term2_dimension.equals("x"))
            x_dimention_term = sqlTerm2;
        else if(term2_dimension.equals("y"))
            y_dimention_term = sqlTerm2;
        else if(term2_dimension.equals("z"))
            z_dimention_term = sqlTerm2;

        if(term3_dimension.equals("x"))
            x_dimention_term = sqlTerm3;
        else if(term3_dimension.equals("y"))
            y_dimention_term = sqlTerm3;
        else if(term3_dimension.equals("z"))
            z_dimention_term = sqlTerm3;

        String octreePath = "src/resources/tables/" + tableName + "/indices/" + targetIndexName + ".ser";
        Octree octree = (Octree) deserialize(octreePath);
        Set<Integer> resultPages = new HashSet<>();
        Methods2.fillSetWithPages_satisfyingCondition_forInputValue(octree, resultPages,
            x_dimention_term._strOperator, y_dimention_term._strOperator, z_dimention_term._strOperator,
            (Comparable) x_dimention_term._objValue,(Comparable) y_dimention_term._objValue,(Comparable) z_dimention_term._objValue);
        serialize(octreePath, octree);
        return resultPages;
    }

    public static void main(String[] args) throws IOException, DBAppException {

        DBApp db = new DBApp();
        //db.createIndex("University", new String[]{"Id", "Name", "Job"});

        // db.DELETETableDependencies("University");

		// Hashtable<String, String> htNameType = new Hashtable<>();
		// htNameType.put("Id", "java.lang.Integer");
		// htNameType.put("Name", "java.lang.String");
		// htNameType.put("Job", "java.lang.String");
		// Hashtable<String, String> htNameMin = new Hashtable<>();
		// htNameMin.put("Id", "1");
		// htNameMin.put("Name", "AAA");
		// htNameMin.put("Job", "aaa");
		// Hashtable<String, String> htNameMax = new Hashtable<>();
		// htNameMax.put("Id", "1000");
		// htNameMax.put("Name", "zz");
		// htNameMax.put("Job", "zzz");
		// db.createTable("University", "Id", htNameType, htNameMin, htNameMax);


        // // generating 20 records
        // Hashtable<String,Object>[] records = new Hashtable[20];
        // for(int i = 0; i < records.length; i++)
        //     records[i] = new Hashtable<String, Object>();
        // String[] names = {"ahmad", "mohamed", "ali", "omar", "zaky", "khaled", "hassan", "hussain", "youssef", "yassin",
        //                    "akrm", "bebo", "loai", "hashem", "mona", "khadija", "bola", "hamdi", "wael", "sharkawy" };
        // String[] jobs = {"doctor", "engineer", "lawyer", "teacher", "policeman", "firefighter", "dentist", "nurse", "farmer", "pilot",
        //                    "blacksmith", "carpenter", "plumber", "electrician", "mechanic", "architect", "designer", "artist", "chef", "waiter" };
        // for(int i = 0; i < records.length; i++)
        // {
        //     Hashtable<String, Object> hashtable = records[i];
        //     hashtable.put("Id", i+2);
        //     hashtable.put("Name", names[i]);
        //     hashtable.put("Job", jobs[i]);
        //     db.insertIntoTable("University", hashtable);

        // }





		// SQLTerm[] arrSQLTerms;
        // arrSQLTerms = new SQLTerm[2];
        // arrSQLTerms[0] = new SQLTerm();
        // arrSQLTerms[0]._strTableName = "University";
        // arrSQLTerms[0]._strColumnName = "Id";
        // arrSQLTerms[0]._strOperator = ">";
        // arrSQLTerms[0]._objValue = 10;
        // arrSQLTerms[1] = new SQLTerm();
        // arrSQLTerms[1]._strTableName = "University";
        // arrSQLTerms[1]._strColumnName = "Name";
        // arrSQLTerms[1]._strOperator = "=";
        // arrSQLTerms[1]._objValue = "ahmad";
        // String[] strarrOperators = new String[1];
        // strarrOperators[0] = "OR";
        // Iterator<Row> iterator = db.selectFromTable(arrSQLTerms, strarrOperators);
        // while(iterator.hasNext()){
        //     System.out.println(iterator.next());
        // }
        System.out.println(db.listofCreatedTables.toString());





}
}