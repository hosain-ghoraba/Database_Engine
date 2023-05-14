package M1;
import M2.Axis;
import M2.Octree;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.text.*;

public class Table implements Serializable
{
    private String strTableName;
    private String strClusteringKeyColumn;
    private Vector<Column> vecColumns;

    public Vector<Octree> getIndex() {
        return index;
    }

    private Vector<String> vecPages;
    private Hashtable<String,String> htblColNameType;
    private Hashtable<String,String> htblColNameMin;
    private Hashtable<String,String> htblColNameMax;
    private Hashtable<String,Integer> htblColNameIndex = new Hashtable<>(); //helper
    private int MaximumRowsCountinTablePage, pagesIDcounter = -1;
    private Vector<Octree> index;
    private Vector<Tuple3> indicies;

    ///////////////3-Tuple

    
	static class Tuple3 implements Serializable{
		String X_idx, Y_idx, Z_idx; //column names
		
		public Tuple3(String X_index,String Y_index,String Z_index) {
			X_idx = X_index;
			Y_idx = Y_index;
			Z_idx = Z_index;
		}
		
        public String toString() {
            return "O("+X_idx + ", " + Y_idx + ", " + Z_idx +")";
        }
		
		public String getFilename() {
			return X_idx + Y_idx + Z_idx + "Index.ser";
		}
		
		public boolean isFullIndexGivenInQuery(Hashtable<String, Object> htblcolNameVal) { 
			return htblcolNameVal.containsKey(X_idx) && htblcolNameVal.containsKey(Y_idx) && htblcolNameVal.containsKey(Z_idx);
		}

        public int getNoOfPartialIndexColumns(Hashtable<String, Object> htblcolNameVal) {
            return (htblcolNameVal.containsKey(X_idx)? 1 : 0) + (htblcolNameVal.containsKey(Y_idx)? 1 : 0) + (htblcolNameVal.containsKey(Z_idx)? 1 : 0);
        }

        public Axis[] getGivenAxes(Hashtable<String, Object> htblcolNameVal){

            Axis[] axes = new Axis[getNoOfPartialIndexColumns(htblcolNameVal)];
            int i = 0;
            if(htblcolNameVal.containsKey(X_idx))
                axes[i++] = Axis.X;
            if(htblcolNameVal.containsKey(Y_idx))
                axes[i++] = Axis.Y;
            if(htblcolNameVal.containsKey(Z_idx))
                axes[i++] = Axis.Z;

            return axes;
        }

        public Comparable getValueOfAxis(Axis ax, Hashtable<String, Object> htblcolNameVal) {
            if(ax == Axis.X)
                return (Comparable) htblcolNameVal.get(X_idx);
            else if(ax == Axis.Y)
                return (Comparable) htblcolNameVal.get(Y_idx);
            else
                return (Comparable) htblcolNameVal.get(Z_idx);
        }
			
	}

    ////////////////////



    public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType,
                 Hashtable<String,String> htblColNameMin,Hashtable<String,String> htblColNameMax , int MaximumRowsCountinTablePage ) {

        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        this.MaximumRowsCountinTablePage = MaximumRowsCountinTablePage;
        index = new Vector<>();
        indicies = new Vector<>();
        vecColumns = new Vector<>(); // initially ,the vector size is by default 10


        Enumeration<String> strEnumeration = htblColNameType.keys();

        while (strEnumeration.hasMoreElements()) {
            String strColName = strEnumeration.nextElement();
            String strColType = htblColNameType.get(strColName);
            String minVal = htblColNameMin.get(strColName);
            String maxVal = htblColNameMax.get(strColName);


            if (strClusteringKeyColumn.equals(strColName))
                this.vecColumns.add(0,new Column(strColName,strColType,true,minVal,maxVal));
            else
                this.vecColumns.add(new Column(strColName, strColType, false, minVal, maxVal));

        } // end while

        this.vecPages = new Vector<>(0);
        
        
        for (int i = 0; i < vecColumns.size(); i++) {
        	Column column = vecColumns.get(i);
			htblColNameIndex.put(column.getStrColName(), i);
		}


    }
    public  Column getColumn(String colName){
//        for (Column c: getVecColumns()) {
//           if(c.getStrColName().equals(colName))
//               return c;
//        }
//        return null;
    	int index = getColumnEquivalentIndex(colName); //O(1)
    	return (index == -1)? null : getVecColumns().get(index);
    }
    public void validateValueType(Column column ,Object valueToCheck, String colType) throws DBAppException{
    	// colType is gotten from the CSV file
        if(colType==null)
        	throw new DBAppException("Error reading CSV file");
    	if(valueToCheck instanceof  String){
                  if(!colType.equals("java.lang.String"))
                      throw new DBAppException("The value and its corresponding column must be of the same type");
                  String value =(String) valueToCheck;
                  if(value.compareTo(column.getStrMaxVal()) > 0 ||value.compareTo(column.getStrMinVal()) < 0 )
                      throw new DBAppException("You cannot use a value that is out of the bounds of this column");
        }
        else if(valueToCheck instanceof  Integer){
            if(!colType.equals("java.lang.Integer"))
                throw new DBAppException("The value and its corresponding column must be of the same type");
            Integer value =(Integer) valueToCheck;
            if(value.compareTo(Integer.valueOf(column.getStrMaxVal())) > 0 ||value.compareTo(Integer.valueOf(column.getStrMinVal())) < 0 )
                throw new DBAppException("You cannot use a value that is out of the bounds of this column");
        }
        else if(valueToCheck instanceof  Double){
            if(!colType.equals("java.lang.Double"))
                throw new DBAppException("The value and its corresponding column must be of the same type");
            Double value =(Double) valueToCheck;
            if(value.compareTo(Double.valueOf(column.getStrMaxVal())) > 0 ||value.compareTo(Double.valueOf(column.getStrMinVal())) < 0 )
                throw new DBAppException("You cannot use a value that is out of the bounds of this column");
        }
        else if (valueToCheck instanceof Date){
            if(!colType.equals("java.util.Date"))
                throw new DBAppException("The value and its corresponding column must be of the same type");
            Date value =(Date) valueToCheck;

            Date dMax, dMin;
            try {
                dMax = new SimpleDateFormat("yyyy/MM/dd").parse(column.getStrMaxVal());
                dMin = new SimpleDateFormat("yyyy/MM/dd").parse(column.getStrMinVal());
            } catch (ParseException e) {            
                throw new DBAppException("Error parsing date");
            }//instead of throwing parseException

            if(value.compareTo(dMax) > 0 ||value.compareTo(dMin) < 0 )
                throw new DBAppException("You cannot usa a value that is out of determined bounds of this column");
        
        } else if(valueToCheck instanceof NULL) {
            if(column.isPrimary()){
                throw new DBAppException("Primary key cannot be a null value");
            }
        }
        else
            throw new DBAppException("This value type is not supported");

    }
    public void validateColType(String colType) throws DBAppException{
        if (!(colType.equals("java.lang.Integer") || colType.equals("java.lang.String") ||
                colType.equals("java.lang.Double") || colType.equals("java.util.Date")))
            throw new DBAppException("The type " + colType + " is not supported");

    }

    public Object getValueBasedOnType(String valueObj, Column column) throws DBAppException {
//        validateValueType(column, valueObj);

        if(column.getStrColType().equals("java.lang.String"))
            return valueObj;
        else if(column.getStrColType().equals("java.lang.Integer"))
            return Integer.valueOf(valueObj);
        else if(column.getStrColType().equals("java.lang.Double"))
            return Double.valueOf(valueObj);
        else if(column.getStrColType().equals("java.lang.Date"))
            try {
                return new SimpleDateFormat("yyyy/MM/dd").parse(valueObj);
            } catch (ParseException e) {
                throw new DBAppException("Error parsing date");
            }
        else
            return null;
    }
        
    public void insertAnEntry(Row entry) throws DBAppException{
        if(vecPages.size() == 0){ // table has no page
            Page page = new Page(strTableName,pagesIDcounter + 1);
            this.addNewPage(page);
            page.insertAnEntry(entry);
            this.savePageToDisk(page, vecPages.size()-1);
        }
        else{ // there are some pages in the table
            Object clustringKey = entry.getData().get(0) ;
            // need to determine the id of the right page to insert the entry into .
            int pidOfCorrectPage = binarySrch(clustringKey) ;


            Page correctPage = loadPage(pidOfCorrectPage);

            if(!correctPage.isFull()) { // fortunately , there is a place
                correctPage.insertAnEntry(entry);
                this.savePageToDisk(correctPage, vecPages.size()-1);
                return;
            }
            else{  // Unfortunately , page is full
                if(pidOfCorrectPage == vecPages.size()-1  ) { //correct page is last page and it is full
                    Page newPage = new Page(strTableName, pagesIDcounter + 1);
                    this.addNewPage(newPage);

                    if(((Comparable)clustringKey).compareTo(correctPage.getMaxValInThePage()) >= 0)
                         newPage.insertAnEntry(entry);
                    else{
                        Row tmp = correctPage.getData().remove(MaximumRowsCountinTablePage-1);
                        correctPage.insertAnEntry(entry);
                        newPage.insertAnEntry(tmp);
                    }
                    this.savePageToDisk(newPage, vecPages.size()-1);
                    this.savePageToDisk(correctPage, vecPages.size()-2);
                    return;
                }else { // correct page is not last page and it is full
                      int i ;
                      for ( i = pidOfCorrectPage; i <vecPages.size();i++) // check other next pages , if they are all full or not
                          if(loadPage(i).getNoOfCurrentRows() < MaximumRowsCountinTablePage)
                              break;

                      Row tmp;
                      if(i==vecPages.size()) {   // all next pages are full , no option other than creating new page
                          Page newPage = new Page(strTableName, pagesIDcounter+1);
                          this.addNewPage(newPage);
                          for(int j = vecPages.size()-1 ; j> pidOfCorrectPage ; j--) {
                        	  Page fromPage = this.loadPage(j-1), toPage = this.loadPage(j);
                              tmp = fromPage.getData().remove(MaximumRowsCountinTablePage - 1);
                              toPage.insertAnEntry(tmp);
                              this.savePageToDisk(fromPage, j-1);
                              this.savePageToDisk(toPage, j);
                          }
                      }
                      else{                     // some page has an empty place
                          for (int j = i; j > pidOfCorrectPage ; j--) {
                        	  Page fromPage = this.loadPage(j-1), toPage = this.loadPage(j);
                              tmp = fromPage.getData().remove(MaximumRowsCountinTablePage - 1);
                              toPage.insertAnEntry(tmp);
                              this.savePageToDisk(fromPage, j-1);
                              this.savePageToDisk(toPage, j);
                          }
                      }
                      correctPage = this.loadPage(pidOfCorrectPage);
                      correctPage.insertAnEntry(entry);
                      this.savePageToDisk(correctPage, pidOfCorrectPage);
                      return;
                }
            }
        }

    }


//consider these cases in binary searching
/*
----insert 5 in:
_____2,3
-
2
3
-
____6,8
-
6
7
8
___9,10
9
10
-
-
______
*/

/*
-----insert 5 in:
_____1,4
1
2
3
4
____6,9
6
7
8
9
___10,11
10
11
-
-
______
*/	
/* insertion in page sizes 2,3,4 and others in old code */

	//a method to binary search from the pages vector to find the page in which the entry with the given key can be inserted

	public int binarySrch(Object key) throws DBAppException {
		int lo = 0;
		int hi = vecPages.size() - 1;
		int mid = 0;
		while (lo <= hi) {
			mid = (lo + hi) / 2;
			Page p = this.loadPage(mid);
			if(((Comparable) key).compareTo(p.getMaxValInThePage()) <= 0 && ((Comparable) key).compareTo(p.getMinValInThePage()) >= 0)
				return mid;//key within range of page
			else if (((Comparable) key).compareTo(p.getMaxValInThePage()) > 0)
				lo = mid + 1;
			else if (((Comparable) key).compareTo(p.getMinValInThePage()) < 0)
				hi = mid - 1;

//			else  return mid;
		}

        //exited loop without finding a page that contains the key in range
		if(hi == -1){ // reached lower bound (by reaching condition that mid is 0 and key is less than the smallest value in the page so automatically it is less than the min value in the page, then the key is the minimum one)
			return 0;

		} else if(lo == vecPages.size()){ // reached upper bound (same for lower bound but vice versa)
			return vecPages.size() - 1;

		} else if(lo > hi){ 		// NOTE that: we reached a case that lo exceeds hi, so lo is the higher page and hi is the lower page
			if(((Comparable) key).compareTo(loadPage(hi).getMaxValInThePage()) >= 0  // key is greater than the max value of the page at hi
				&& ((Comparable) key).compareTo(loadPage(lo).getMinValInThePage()) <= 0){// key is greater than the min value of the page at lo
					return (!loadPage(hi).isFull()) ? hi : lo;
                        // if the page at hi is not full, return hi, otherwise return lo
				}
                ///////////// example of the case above: insert 5 in:  hi->Page0(1,2,4);  lo->Page1(6,9,10,11)
                ///// first iteration was lo = mid = 0, hi =1
                ///// second iteration was lo = 1 = hi = mid
                ///// 3rd loop iteration was lo = 1, hi = '0'
                ////////so we check that condition

		}

		return mid;
	}

    public Page loadPage(int index) throws DBAppException {
    	if(vecPages.size() == 0)
            throw new DBAppException("Table is Empty & Cannot perform any CRUD operations");
        if(index < 0 || index >= vecPages.size())
            throw new DBAppException("Page index not valid or out of bounds");

        String path ="src/resources/tables/"+strTableName+ "/pages/" + vecPages.get(index) + ".ser";
        Page page = (Page) DBApp.deserialize(path);

        return page;
    }

    public Page loadPageByPID(int pid) throws DBAppException {
    	if(vecPages.size() == 0)
            throw new DBAppException("Table is Empty & Cannot perform any CRUD operations");
        if(pid < 0 || pid > pagesIDcounter)
            throw new DBAppException("Page index not valid or out of bounds");

        String path ="src/resources/tables/"+strTableName+ "/pages/page" + pid + ".ser";
        Page page = (Page) DBApp.deserialize(path);

        return page;
    }
    
    public void savePageToDisk( Page page , int pageIndex) throws DBAppException {
        String path ="src/resources/tables/"+strTableName+ "/pages/page" + page.getPid() + ".ser";
        DBApp.serialize( path,page );
    }
    public void addNewPage(Page newPage) throws DBAppException { // add new page to the vector of pages
        vecPages.add("page" + ++pagesIDcounter); // add file name\path to the vector of pages 
        savePageToDisk(newPage, pagesIDcounter); // save the page to disk
    }

    public String getStrTableName() {
        return strTableName;
    }

    public void setStrTableName(String strTableName) {
        this.strTableName = strTableName;
    }

    public String getStrClusteringKeyColumn() {
        return strClusteringKeyColumn;
    }

    public void setStrClusteringKeyColumn(String strClusteringKeyColumn) {
        this.strClusteringKeyColumn = strClusteringKeyColumn;
    }

    public Vector<Column> getVecColumns() {
        return vecColumns;
    }

    public void setVecColumns(Vector<Column> vecColumns) {
        this.vecColumns = vecColumns;
    }

    public Vector<String> getVecPages() {
        return vecPages;
    }

    public void setVecPages(Vector<String> vecPages) {
        this.vecPages = vecPages;
    }

    public Vector<Tuple3> getIndices(){
        return indicies;
    }

    public Hashtable<String, String> getHtblColNameType() {
        return htblColNameType;
    }

    public void setHtblColNameType(Hashtable<String, String> htblColNameType) {
        this.htblColNameType = htblColNameType;
    }

    public Hashtable<String, String> getHtblColNameMin() {
        return htblColNameMin;
    }

    public void setHtblColNameMin(Hashtable<String,String> htblColNameMin) {
        this.htblColNameMin = htblColNameMin;
    }

    public Hashtable<String, String> getHtblColNameMax() {
        return htblColNameMax;
    }

    public void setHtblColNameMax(Hashtable<String,String> htblColNameMax) {
        this.htblColNameMax = htblColNameMax;
    }
    
    public int getColumnEquivalentIndex(String colName) {
    	//gets index or returns -1 in case it doesn't exist
		return htblColNameIndex.getOrDefault(colName, -1);
	}

    /*public int getNextNewPageIDToBeCreated() {
        return pagesIDcounter+1;
    }*/

    public String  toString(){
        String strTblOutput = getStrTableName() + " Table \n"  + "-------------------------------" + "\n";
        Iterator iterateOverColumns = getVecColumns().iterator();
        while (iterateOverColumns.hasNext()) {
            String tmp = String.valueOf(iterateOverColumns.next());
            strTblOutput += tmp ;
            if(tmp.equals(strClusteringKeyColumn))
                strTblOutput += "*";
            strTblOutput+= "\t| ";
        }
        strTblOutput+="\n" +"-------------------------------" +"\n";


        for (int i = 0; i < vecPages.size(); i++) {
            Page page;
            try {
                page = this.loadPage(i);
                strTblOutput += "page" + /*(page.getPid())*/ i + "\t\t-filename on disk[page" + page.getPid() + ".ser]\n" +"-------"+"\n" + page.toString()+"\n";
            } catch (DBAppException e) {
                e.printStackTrace();
            }
        }

        return strTblOutput;

    }
    
    
    public int findRowToUpdORdel(Object key, int candidateIdx, boolean isPID_falseForVectorIndex_trueForPIDonDisk) throws DBAppException {
		Vector<Row> candidatePageData;
        if(isPID_falseForVectorIndex_trueForPIDonDisk)
            candidatePageData = this.loadPageByPID(candidateIdx).getData();
        else
            candidatePageData = this.loadPage(candidateIdx).getData();


		//binary searching on row to be updated or deleted
        int lo = 0, hi = candidatePageData.size() - 1;
        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2;
            if (((Comparable) key).compareTo(candidatePageData.get(mid).getData().get(getColumnEquivalentIndex(strClusteringKeyColumn))) < 0)
                hi = mid - 1;
            else if (((Comparable) key).compareTo(candidatePageData.get(mid).getData().get(getColumnEquivalentIndex(strClusteringKeyColumn))) > 0)
                lo = mid + 1;
            else
                return mid;
        }
		
		return -1;
	}

    private String getClusteringIndexName() /* if exists, otherwise null */ throws DBAppException{
        BufferedReader br;
		try {
            br = new BufferedReader(new FileReader("MetaData.csv"));
            String line ;
            br.readLine();
            line = br.readLine();
            while (line != null) {
                String[] values = line.split(",");
                if(values[0].equals(strTableName) && values[3].contains("true") /* this is ,y clustering key of the table */){
                    if(!values[5].contains("null")) {
                        br.close();
                        return values[4];
                    } else{ br.close(); return null;}
                }
                line = br.readLine();
            }

            br.close();

            return null;
        } catch ( IOException e2) {
            throw new DBAppException("Error reading csv file");
        }
    }

    public void updateTableRow(Object clstrKeyVal, Hashtable<String, Object> colNameVal) throws DBAppException {
        String clusteringIndexName = getClusteringIndexName();
        LinkedList<Integer> addressedPagesID;
        if(clusteringIndexName != null) {
            //the page(s) to be deleted are the ones that are got from the clustering index
            
            Tuple3 tupIndex = null;
            for (Tuple3 tuple : indicies) {
                if(tuple.X_idx.equals(strClusteringKeyColumn)){
                    tupIndex = tuple;
                    break;
                }
            }

            Hashtable<String,Object> htblclusterKeyVal = new Hashtable<>();
            htblclusterKeyVal.put(strClusteringKeyColumn, clstrKeyVal);

            
            Octree tree = (Octree) DBApp.deserialize("src/resources/tables/"+ strTableName + "/Indicies/"+tupIndex.getFilename());

            Axis[] axis = tupIndex.getGivenAxes(colNameVal); //guaranteed to be 1 axis only
            addressedPagesID = tree.getPagesAtPartialPoint((Comparable)clstrKeyVal, axis[0]);

            
        } else {
            //the page(s) to be deleted is the one that is got from Pages BS & have the clustering key value

            addressedPagesID = new LinkedList<>();
            int candidateIdx = this.binarySrch(clstrKeyVal);

            try {
                addressedPagesID.add( Integer.parseInt(vecPages.get(candidateIdx).split("page")[1])  ); //extract page id from name
            } catch (Exception e) {
                throw new DBAppException("Error reading page and extracting data");
            }
        }

        
            Set<Integer> checkedPages = ConcurrentHashMap.newKeySet();

            // TODO : update the row in the index  //update : I guess done, I hope
            for (Integer pid : addressedPagesID) {
                if(checkedPages.contains(pid)) continue;
                checkedPages.add(pid);

                
                int rowIdxToUpdate = this.findRowToUpdORdel(clstrKeyVal, pid , true);
                if (rowIdxToUpdate < 0) 
                    continue;
                else {
                    //row and page found to be updated
                    Page candidatePage = this.loadPageByPID(pid);
                    candidatePage.updateRow(this,rowIdxToUpdate, colNameVal);
                    this.savePageToDisk(candidatePage, pid);
                    return;
                }
            }

            
            System.out.println("No such row matches to update it");
            return;
    }


    private boolean inputHasIndex(Hashtable<String, Object> colNameVal) throws DBAppException{
        BufferedReader br;
		try {
            br = new BufferedReader(new FileReader("MetaData.csv"));
            String line ;
            br.readLine();
            line = br.readLine();
            while (line != null) {
                String[] values = line.split(",");
                if(values[0].equals(strTableName) && values[4].contains("Index") && colNameVal.containsKey(values[1])){
                    br.close();
                    return true;
                }
                line = br.readLine();
            }

            br.close();

            return false;
        } catch ( IOException e2) {
            throw new DBAppException("Error reading csv file");
        }
    }

    public int deleteRowWITHCKey(Object clusteringKeyVal ,Hashtable<String, Object> colNameVal) throws DBAppException{
        LinkedList<Integer> addressedPagesID;

        if(inputHasIndex(colNameVal)){
            //get the pages to be deleted from the index
            addressedPagesID = getPagesToDeleteWithIndex(colNameVal);
            
        }
        else {
            //the page(s) to be deleted is the one that is got from Pages BS & have the clustering key value
            addressedPagesID = new LinkedList<>();
            int candidateIdx = this.binarySrch(clusteringKeyVal);

            try {
                addressedPagesID.add( Integer.parseInt(vecPages.get(candidateIdx).split("page")[1])  ); //extract page id from name
            } catch (Exception e) {
                throw new DBAppException("Error reading page and extracting data or Table empty");
            }
        }

        // old normal code of binary searching on Page Rows
        // once you find the row to delete, break; since it's unique

        
        Set<Integer> checkedPages = ConcurrentHashMap.newKeySet();

        //TODO: delete from index //update : I guess done, I hope
        for (Integer pid : addressedPagesID) {
            if(checkedPages.contains(pid)) continue;
            checkedPages.add(pid);
            
            
            int rowtodelete = this.findRowToUpdORdel(clusteringKeyVal, pid, true);
            if (rowtodelete < 0)
                continue;
            else{
                Page candidatePage = this.loadPage(pid);
                Row row = candidatePage.deleteEntry(rowtodelete);
                this.savePageToDisk(candidatePage, pid);

                // delete row from all table octree indices
                
                Vector<Tuple3> indices = this.getIndices();

                for (Tuple3 tuple : indices) {
            
                    Octree tree = (Octree) DBApp.deserialize("src/resources/tables/"+ strTableName + "/Indicies/"+ tuple.getFilename());
                    Comparable Xobj = (Comparable) row.getData().get(this.getColumnEquivalentIndex(tuple.X_idx));
                    Comparable Yobj = (Comparable) row.getData().get(this.getColumnEquivalentIndex(tuple.Y_idx));
                    Comparable Zobj = (Comparable) row.getData().get(this.getColumnEquivalentIndex(tuple.Z_idx));
                    tree.deletePageFromTree( Xobj, Yobj, Zobj, pid);
                    
                    DBApp.serialize("src/resources/tables/"+ strTableName + "/Indicies/"+ tuple.getFilename(), tree);
                }

                return 1;
            }

        }
        
        System.out.println("No rows matches these conditions.");
        return 0;
    }
    
    public int deleteRowsWithoutCKey(Hashtable<String, Object> colNameVal) throws DBAppException {
        LinkedList<Integer> addressedPagesID;
        if(inputHasIndex(colNameVal)){
            //get the pages to be deleted from the index
            addressedPagesID = getPagesToDeleteWithIndex(colNameVal);
            
        } else{

            //formulate and initiate the pagesIds with the one in vecPages to linear scan the whole table
            addressedPagesID = new LinkedList<>();
            try{
                for (int i = 0; i < vecPages.size(); i++) {
                    addressedPagesID.add( Integer.parseInt(vecPages.get(i).split("page")[1])  ); //extract page id from name
                }
            } catch (Exception e) {
                throw new DBAppException("Error reading page and extracting data or Table empty");
            }
        }

        //continue to scan linearly with old code
        ///////////////////////////////////////////TODO Delete record from Octree Index //update : I guess done, I hope
    	int items = 0;
        Set<Integer> checkedPages = ConcurrentHashMap.newKeySet();

		for (int i = 0; i < addressedPagesID.size(); i++) {
            if(checkedPages.contains(addressedPagesID.get(i))) continue;

            int pid = addressedPagesID.get(i);
            checkedPages.add(pid);
            
            Page page = loadPageByPID(pid);
			Iterator<Row> iterator = page.getData().iterator();
			while (iterator.hasNext()) {
				Row row = (Row) iterator.next();
				boolean ANDING = true;
				
				Enumeration<String> strEnumeration = colNameVal.keys();
				while (strEnumeration.hasMoreElements()) {
					String strColName = strEnumeration.nextElement();
					if(!row.getData().get(getColumnEquivalentIndex(strColName)).equals(colNameVal.get(strColName))) {
						ANDING = false;
						break;
					}else ANDING = true;
				}
				
				if(ANDING /*is true*/) {	
					iterator.remove(); //same as page.deleteEntry(row)
					items++;

                    // delete row from all table octree indices
                    Vector<Tuple3> indices = this.getIndices();

                    for (Tuple3 tuple : indices) {
                
                        Octree tree = (Octree) DBApp.deserialize("src/resources/tables/"+ strTableName + "/Indicies/"+ tuple.getFilename());
                        Comparable Xobj = (Comparable) row.getData().get(this.getColumnEquivalentIndex(tuple.X_idx));
                        Comparable Yobj = (Comparable) row.getData().get(this.getColumnEquivalentIndex(tuple.Y_idx));
                        Comparable Zobj = (Comparable) row.getData().get(this.getColumnEquivalentIndex(tuple.Z_idx));
                        tree.deletePageFromTree( Xobj, Yobj, Zobj, pid);
                        
                        DBApp.serialize("src/resources/tables/"+ strTableName + "/Indicies/"+ tuple.getFilename(), tree);
                    }
				}
			}
			this.savePageToDisk(page, i);
		}
    	
    	
    	return items;
	}

    private LinkedList<Integer> getPagesToDeleteWithIndex(Hashtable<String, Object> colNameVal) throws DBAppException {
        //Delete with index logic - either full or partial

        String indexFilename = "";
        Tuple3 correctindex = null;
		for (Tuple3 tuple : indicies) {
            if(tuple.isFullIndexGivenInQuery(colNameVal)){
                indexFilename = tuple.getFilename();
                correctindex = tuple;
            }
        }

        if(indexFilename.length() == 0){ // get pages to delete with partial index and return
            return getPagesToDeleteWithPartialIndex(colNameVal);
        }

        //delete with full index logic ....
        Octree tree = (Octree) DBApp.deserialize("src/resources/tables/"+ strTableName + "/Indicies/"+indexFilename);

        LinkedList<Integer> pages =tree.getPagesAtPoint((Comparable)colNameVal.get(correctindex.X_idx), (Comparable)colNameVal.get(correctindex.Y_idx), (Comparable)colNameVal.get(correctindex.Z_idx));
        return pages;
    }


    private LinkedList<Integer> getPagesToDeleteWithPartialIndex(Hashtable<String, Object> colNameVal) throws DBAppException{
        //Delete with partial index logic    

        // 1- get the index with the most columns given in the query (prefer 2 columns over 1 column)

        int partialidxNO = 0;
        Tuple3 correctindex = null;

        for (Tuple3 tuple : indicies) {
            int check = tuple.getNoOfPartialIndexColumns(colNameVal);
            if(check > partialidxNO){
                partialidxNO = check;
                correctindex = tuple;

                if(partialidxNO == 2) break; // Since we got to this piece of code, we are sure that there is no full index given in the query
                                            // and since the max number of columns in a partial index is 2, we can break, and use this index
            }
        }

        if(correctindex == null) return null; // no index to delete from
        
        // 2- get the pages to be deleted from the index
        
        Octree tree = (Octree) DBApp.deserialize("src/resources/tables/"+ strTableName + "/Indicies/"+correctindex.getFilename());

        Axis[] axes = correctindex.getGivenAxes(colNameVal); // get the index axes that are given in the query

        if(partialidxNO == 1){
            LinkedList<Integer> pages = tree.getPagesAtPartialPoint(correctindex.getValueOfAxis(axes[0], colNameVal), axes[0]);
            return pages;
        } else if(partialidxNO == 2){
            LinkedList<Integer> pages = tree.getPagesAtPartialPoint(correctindex.getValueOfAxis(axes[0], colNameVal), axes[0]
                                                                    , correctindex.getValueOfAxis(axes[1], colNameVal), axes[1]);
            
            return pages;
        }

        return null;
    }

}

