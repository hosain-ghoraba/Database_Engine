import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.text.*;

public class Table implements Serializable
{
    private String strTableName;
    private String strClusteringKeyColumn;
    private Vector<Column> vecColumns;
    private Vector<Page> vecPages;
    private Hashtable<String,String> htblColNameType;
    private Hashtable<String,String> htblColNameMin;
    private Hashtable<String,String> htblColNameMax;
    private int MaximumRowsCountinTablePage;


    public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType,
                 Hashtable<String,String> htblColNameMin,Hashtable<String,String> htblColNameMax , int MaximumRowsCountinTablePage ) {

        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        this.MaximumRowsCountinTablePage = MaximumRowsCountinTablePage;
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

    }
    public  Column getColumn(String colName){
        for (Column c: getVecColumns()) {
           if(c.getStrColName().equals(colName))
               return c;
        }
        return null;
    }
    public void validateValueType(Column column ,Object valueToCheck) throws DBAppException,ParseException{
        if(valueToCheck instanceof  String){
                  if(!column.getStrColType().equals("java.lang.String"))
                      throw new DBAppException("The value and its corresponding column must be of the same type");
                  String value =(String) valueToCheck;
                  if(value.compareTo(column.getStrMaxVal()) > 0 ||value.compareTo(column.getStrMinVal()) < 0 )
                      throw new DBAppException("You cannot insert a value that is out of the bounds of this column");
        }
        else if(valueToCheck instanceof  Integer){
            if(!column.getStrColType().equals("java.lang.Integer"))
                throw new DBAppException("The value and its corresponding column must be of the same type");
            Integer value =(Integer) valueToCheck;
            if(value.compareTo(Integer.valueOf(column.getStrMaxVal())) > 0 ||value.compareTo(Integer.valueOf(column.getStrMinVal())) < 0 )
                throw new DBAppException("You cannot insert a value that is out of the bounds of this column");
        }
        else if(valueToCheck instanceof  Double){
            if(!column.getStrColType().equals("java.lang.Double"))
                throw new DBAppException("The value and its corresponding column must be of the same type");
            Double value =(Double) valueToCheck;
            if(value.compareTo(Double.valueOf(column.getStrMaxVal())) > 0 ||value.compareTo(Double.valueOf(column.getStrMinVal())) < 0 )
                throw new DBAppException("You cannot insert a value that is out of the bounds of this column");
        }
        else if (valueToCheck instanceof Date){
            if(!column.getStrColType().equals("java.lang.Date"))
                throw new DBAppException("The value and its corresponding column must be of the same type");
            Date value =(Date) valueToCheck;

            Date dMax = new SimpleDateFormat("yyyy/MM/dd").parse(column.getStrMaxVal());//throw parseException
            Date dMin = new SimpleDateFormat("yyyy/MM/dd").parse(column.getStrMinVal());

            if(value.compareTo(dMax) > 0 ||value.compareTo(dMin) < 0 )
                throw new DBAppException("You cannot insert a value that is out of determined bounds of this column");

        }
        else
            throw new DBAppException("This value type is not supported");

    }
    public void insertAnEntry(Row entry) throws DBAppException{
        if(vecPages.size() == 0){ // table has no page
            Page page = new Page(strTableName,0);
            vecPages.add(page);
            page.insertAnEntry(entry);
        }
        else{ // there are some pages in the table
            Object clustringKey = entry.getData().get(0) ;
            // need to determine the id of the right page to insert the entry into .
            int pidOfCorrectPage = binarySrch(clustringKey) ;


            Page correctPage = vecPages.get(pidOfCorrectPage);

            if(correctPage.getNoOfCurrentRows() < MaximumRowsCountinTablePage) { // fortunately , there is a place
                correctPage.insertAnEntry(entry);
                return;
            }
            else{  // Unfortunately , page is full
                if(pidOfCorrectPage == vecPages.size()-1  ) { //correct page is last page and it is full
                    Page newPage = new Page(strTableName, vecPages.size());
                    vecPages.add(newPage);
                    if(((Comparable)clustringKey).compareTo(correctPage.getMaxValInThePage()) >= 0)
                         newPage.insertAnEntry(entry);
                    else{
                        Row tmp = correctPage.getData().remove(MaximumRowsCountinTablePage-1);
                        correctPage.insertAnEntry(entry);
                        newPage.insertAnEntry(tmp);
                    }
                    return;
                }else { // correct page is not last page and it is full
                      int i ;
                      for ( i = pidOfCorrectPage; i <vecPages.size();i++) // check other next pages , if they are all full or not
                          if(vecPages.get(i).getNoOfCurrentRows() < MaximumRowsCountinTablePage)
                              break;

                      Row tmp;
                      if(i==vecPages.size()) {   // all next pages are full , no option other than creating new page
                          Page newPage = new Page(strTableName, vecPages.size());
                          vecPages.add(newPage);
                          for(int j = vecPages.size()-1 ; j> pidOfCorrectPage ; j--) {
                              tmp = vecPages.get(j-1).getData().remove(MaximumRowsCountinTablePage - 1);
                              vecPages.get(j).insertAnEntry(tmp);
                          }
                      }
                      else{                     // some page has an empty place
                          for (int j = i; j > pidOfCorrectPage ; j--) {
                              tmp = vecPages.get(j-1).getData().remove(MaximumRowsCountinTablePage - 1);
                              vecPages.get(j).insertAnEntry(tmp);
                          }
                      }
                      correctPage.insertAnEntry(entry);
                      return;
                }
            }
        }

    }

    public int binarySrch(Object obj) {
        int left = 0;
        int right = vecPages.size()-1;
        int middle = (left + right) / 2;
        Page p = vecPages.get(middle);
        while (left != right+1) {

                if(((Comparable) obj).compareTo(p.getMaxValInThePage()) <= 0  && ((Comparable) obj).compareTo(p.getMinValInThePage()) > 0 || ((Comparable) obj).compareTo(p.getMaxValInThePage()) < 0  && ((Comparable) obj).compareTo(p.getMinValInThePage()) >= 0 )
                        return middle;

                //if the pointer points to the last page and the key is larger than the largest value in that page
                if(middle == vecPages.size()-1)
                    if(((Comparable) obj).compareTo(p.getMaxValInThePage()) >= 0)
                        return middle;

                //if the pointer points to the first page and the key is smaller than the smallest value in that page
               if(middle == 0)
                if(((Comparable) obj).compareTo(p.getMaxValInThePage()) <= 0)
                    return middle;


               // the above 3 if condition can be combined only in 1 if condition using ORing , but I preferred to split them for simplicity

                if (((Comparable) obj).compareTo(p.getMaxValInThePage()) > 0)
                    left = middle + 1;
                else if (((Comparable) obj).compareTo(p.getMaxValInThePage()) < 0)
                    right = middle - 1;
                middle = (left + right) / 2;
        }

        return -1;
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

    public Vector<Page> getVecPages() {
        return vecPages;
    }

    public void setVecPages(Vector<Page> vecPages) {
        this.vecPages = vecPages;
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

        for (Page x: getVecPages()) {
            strTblOutput += "page"+x.getPid() + "\n" +"-------"+"\n"+x.toString()+"\n";
        }

        return strTblOutput;

    }

}

