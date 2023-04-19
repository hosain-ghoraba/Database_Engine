package M1;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Vector;

public class Row implements Serializable,Comparable {
    private Vector<Object> rowData ;

    public Row(Vector<Object> d){
       rowData = d ;

    }
    public int compareTo(Object x){
        Row other = (Row) x;
        return ((Comparable)this.rowData.get(0)).compareTo(other.rowData.get(0));

    }

    public Vector<Object> getData() {
        return rowData;
    }

    public void setData(Vector<Object> data) {
        this.rowData = data;
    }

    public String toString(){
        String rowOutput = "";
        Iterator iterateOverData  = getData().iterator();
        while (iterateOverData.hasNext())
            rowOutput += iterateOverData.next() + "\t \t"  ;

        return rowOutput;
    }
}
