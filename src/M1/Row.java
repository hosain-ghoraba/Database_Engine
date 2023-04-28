package M1;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Vector;

public class Row implements Serializable,Comparable<Row> {
    private Vector<Object> rowData ;

    public Row(Vector<Object> d){
       rowData = d ;

    }

    @Override
    public boolean equals(Object obj) {
    	Row r;
    	if( obj instanceof Row) r = (Row) obj;
    	else return false;

		return this.compareTo(r) == 0;
	}
    
    public int compareTo(Row x){
        Row other = (Row) x;
        if(x instanceof NULL) return 1;
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
        Iterator<Object> iterateOverData  = getData().iterator();
        while (iterateOverData.hasNext())
            rowOutput += iterateOverData.next() + "\t \t"  ;

        return rowOutput;
    }
}
