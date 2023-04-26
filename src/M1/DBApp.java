package M1;

import java.io.*;
import java.text.ParseException;
import java.util.*;

import javax.print.DocFlavor.STRING;
import javax.swing.text.html.HTML;

public class DBApp {
    public static int MaximumRowsCountinTablePage;
    public int MaximumEntriesinOctreeNode;
    private LinkedList<String> listofCreatedTables;
    private static long start, end;// for execution time calculation

    public DBApp() {
        listofCreatedTables = new LinkedList<>();
        init();
    }

    public void init() {
        this.readConfig();
    };
    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) throws DBAppException {
        // surroud the whole method with try catch to catch any exception and re-throw it as DBAppException
        try
        {
        validation(htblColNameType, htblColNameMin, htblColNameMax); // validation method checks if the type of table's columns is one of the 4 types specified in the description,validation also checks if any column does not have maxVal or minVal , hence throws exception
        for(String colName : htblColNameMin.keySet()) // converts all min and max column values to lowercase (hosain)
            htblColNameMin.put(colName, htblColNameMin.get(colName).toLowerCase());
        for(String colName : htblColNameMax.keySet())
            htblColNameMax.put(colName, htblColNameMax.get(colName).toLowerCase());
        readConfig();
        Table tblCreated = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin,
                htblColNameMax, MaximumRowsCountinTablePage);
        listofCreatedTables.add(strTableName);
        String strTablePath = "src/resources/tables/" + strTableName;
        File newFile = new File(strTablePath);
        newFile.mkdir();
        newFile = new File(strTablePath + "/pages");
        newFile.mkdir();
        // serialization
        serialize(strTablePath + "/" + strTableName + ".ser", tblCreated);
        }
        catch(Exception e)
        {
            throw new DBAppException(e.getMessage());
        }

    }
    
    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)throws DBAppException {
        // surroud the whole method with try catch to catch any exception and re-throw it as DBAppException
        try
        {
        if (!listofCreatedTables.contains(strTableName))
            throw new DBAppException("You cannot insert into a table that has not been created yet");
        Methods.check_strings_are_Alphabitical(htblColNameValue);
        Methods.convert_strings_To_Lower_Case(htblColNameValue); // convert all string records in hashtable to lower case
        // 1- fetch the table from the disk
        String path = "src/resources/tables/" + strTableName + "/" + strTableName + ".ser";
        Table tblToInsertInto = (Table) deserialize(path);

        // 2- insert a record into it
        Vector<Object> vecValues = new Vector<>();
        Enumeration<String> strEnumeration = htblColNameValue.keys();
        while (strEnumeration.hasMoreElements()) {
            String strColName = strEnumeration.nextElement();
            Column c = tblToInsertInto.getColumn(strColName);
            // if this Column does not exist , throw exception
            if (/*!tblToInsertInto.getVecColumns().contains(c)*/ c == null)
                throw new DBAppException("No such column");
            // get the value
            Object strColValue = htblColNameValue.get(strColName);
            // check the value type
            try {
                tblToInsertInto.validateValueType(c, strColValue);

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
        Row entry = new Row(vecValues);
        tblToInsertInto.insertAnEntry(entry);
        // 3-return table back to disk with the the new inserted value
        serialize(path, tblToInsertInto);
        }
        catch(Exception e)
        {
            throw new DBAppException(e.getMessage());
        }
        

    }
    public void updateTable(String strTableName, String strClusteringKeyValue,Hashtable<String, Object> htblColNameValue) throws DBAppException {
        // surroud the whole method with try catch to catch any exception and re-throw it as DBAppException
       
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
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)throws DBAppException {
        // surroud the whole method with try catch to catch any exception and re-throw it as DBAppException
        try
        {
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
            Column c = tblToUpdate.getColumn(strColName);
            // if this Column does not exist , throw exception
            if (!tblToUpdate.getVecColumns().contains(c))
                throw new DBAppException("No such column");
            // get the value
            Object strColValue = htblColNameValue.get(strColName);
            // check the value type
            tblToUpdate.validateValueType(c, strColValue);

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
            }else index = iteratetmp.nextIndex();
        }

        // 4-return table back to disk after update
        serialize(path, tblToUpdate);
        }
        catch(Exception e)
        {
            throw new DBAppException(e.getMessage());
        }
        
    }

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
    public void readConfig() {
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
            fnfe.printStackTrace();
            System.out.println("Configuration file cannot be found");
        } catch (IOException ioe) {
            ioe.printStackTrace();
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
            i.printStackTrace();
            throw new DBAppException(path + " not found");
        }
        return obj;
    }
    
    // public static Iterator selectFromTable(SQLTerm[] arrSQLTerms,String[]
    // strarrOperators)throws DBAppException{
    // }

    public static void main(String[] args) throws DBAppException, InterruptedException, ParseException {
        starty();

        DBApp d = new DBApp();
        Hashtable<String, String> htNameType = new Hashtable<>();
        htNameType.put("Id", "java.lang.Integer");
        htNameType.put("Name", "java.lang.String");
        htNameType.put("Job", "java.lang.String");
        Hashtable<String, String> htNameMin = new Hashtable<>();
        htNameMin.put("Id", "1");
        htNameMin.put("Name", "AAA");
        htNameMin.put("Job", "blacksmith");
        Hashtable<String, String> htNameMax = new Hashtable<>();
        htNameMax.put("Id", "1000");
        htNameMax.put("Name", "zaky");
        htNameMax.put("Job", "zzz");

        d.createTable("University", "Id", htNameType, htNameMin, htNameMax);
        
        
        Hashtable<String, Object> htColNameVal0 = new Hashtable<>();
        htColNameVal0.put("Id", 23);
        htColNameVal0.put("Name", new String("ahmed"));
        htColNameVal0.put("Job", new String("blacksmith"));

        Hashtable<String, Object> htColNameVal1 = new Hashtable<>();
        htColNameVal1.put("Id", 33);
        htColNameVal1.put("Name", new String("ali"));
        htColNameVal1.put("Job", new String("engineer"));

        Hashtable<String, Object> htColNameVal2 = new Hashtable<>();
        htColNameVal2.put("Id", 11);
        htColNameVal2.put("Name", new String("dani"));
        htColNameVal2.put("Job", new String("doctor"));

        Hashtable<String, Object> htColNameVal3 = new Hashtable<>();
        htColNameVal3.put("Id", 15);
        htColNameVal3.put("Name", new String("basem"));
        htColNameVal3.put("Job", new String("teacher"));

        Hashtable<String, Object> htColNameVal4 = new Hashtable<>();
        htColNameVal4.put("Id", 14);
        htColNameVal4.put("Name", new String("mostafa"));
        htColNameVal4.put("Job", new String("engineer"));

        // Hashtable<String, Object> htColNameVal5 = new Hashtable<>();
        // htColNameVal5.put("Id", 70);
        // htColNameVal5.put("Name", new String("7amood"));
        // htColNameVal5.put("Job", new String("m4 la2i"));

        Hashtable<String, Object> htNameValdelete1 = new Hashtable<>();
        htNameValdelete1.put("Job", new String("engineer"));
        Hashtable<String, Object> htNameValupdate1 = new Hashtable<>();
        htNameValupdate1.put("Name", new String("SeragMohema"));
        htNameValupdate1.put("Job", new String("AmnDawla"));



        // insertion test
        d.insertIntoTable("University", htColNameVal0);///////////////////////

        Table x = (Table) deserialize("src/resources/tables/University/University.ser");
        System.out.println(x.toString());
        d.insertIntoTable("University", htColNameVal2);////////////////////////////////
x = (Table) deserialize("src/resources/tables/University/University.ser");
        System.out.println(x.toString());
		d.insertIntoTable("University", htColNameVal1);///////////////////////////////
x = (Table) deserialize("src/resources/tables/University/University.ser");
        System.out.println(x.toString());
		d.insertIntoTable("University", htColNameVal4);//////////////////////////////
x = (Table) deserialize("src/resources/tables/University/University.ser");
        System.out.println(x.toString());
		d.insertIntoTable("University", htColNameVal3);////////////////////////////
x = (Table) deserialize("src/resources/tables/University/University.ser");
        System.out.println(x.toString());
        
        
        
        // deletion test
         d.deleteFromTable("University", htNameValdelete1);//without PK
//         d.deleteFromTable("University", htColNameVal4);
//		d.deleteFromTable("University", htColNameVal1);
//         d.deleteFromTable("University", htColNameVal0);
//         d.deleteFromTable("University", htColNameVal3);
//         d.deleteFromTable("University", htColNameVal2);
		x = (Table) deserialize("src/resources/tables/University/University.ser");
        System.out.println(x.toString());

        // Update Test
		d.updateTable("University","11", htNameValupdate1);
x = (Table) deserialize("src/resources/tables/University/University.ser");
        System.out.println(x.toString());

        System.out.println("Hello, Database World!");
        // update test
        // System.out.println(x.toString());

        // 0,2,1,3
        // 0,2,3,1
        // 0,3,2,1
        // 0,3,1,2
        // 0,1,2,3
        // 0,1,3,2

        /*
         * Hashtable<String, String> htNameType = new Hashtable<>();
         * htNameType.put("ahmed", "java.lang.String");
         * Hashtable<String, String> htNameMin = new Hashtable<>();
         * Hashtable<String, String> htNameMax = new Hashtable<>();
         * htNameMin.put("ahmed", "2");
         * htNameMax.put("ahmed", "7");
         * 
         * 
         * 
         * try {
         * validation(htNameType, htNameMin, htNameMax);
         * System.out.println("right");
         * }
         * catch (DBAppException e){
         * System.out.println(e.getMessage());
         * }
         * 
         */
        endy();
    }

    // Iterator class
    // Make a collection
    /*
     * ArrayList<String> cars = new ArrayList<String>();
     * cars.add("Volvo");
     * cars.add("BMW");
     * cars.add("Ford");
     * cars.add("Mazda");
     * 
     * // Get the iterator
     * Iterator<String> it = cars.iterator();
     * 
     * // Print the first item
     * while(it.hasNext())
     * System.out.println(it.next());
     * 
     */

    /*
     * ArrayList<Integer> numbers = new ArrayList<Integer>();
     * numbers.add(12);
     * numbers.add(8);
     * numbers.add(2);
     * numbers.add(23);
     * Iterator<Integer> it = numbers.iterator();
     * while(it.hasNext()) {
     * Integer i = it.next();
     * if(i < 10) {
     * it.remove();
     * }
     * }
     * System.out.println(numbers);
     * }
     * Trying to remove items using a for loop or a for-each loop would not work
     * correctly
     * because the collection is changing size at the same time that the code is
     * trying to loop
     * 
     */



}