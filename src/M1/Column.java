package M1;
import java.io.Serializable;

public class Column  implements Serializable {
    private String strColName;
    private String strColType;
    private boolean isPrimary;
    private String strMinVal;
    private String strMaxVal;

    public Column(String strColName ,String strColType , boolean isPrimary , String strMinVal , String strMaxVal){
        this.strColName = strColName;
        this.strColType = strColType;
        this.isPrimary = isPrimary;
        this.strMinVal = strMinVal;
        this.strMaxVal = strMaxVal;
    }

    public String getStrMinVal() {
        return strMinVal;
    }
    public String getStrMaxVal(){ return strMaxVal;}

    public boolean isPrimary() {
        return isPrimary;
    }


    public void setStrMinVal(String strMinVal) {
        this.strMinVal = strMinVal;
    }

    public String getStrColType() {
        return strColType;
    }

    public void setStrColType(String strColType) {
        this.strColType = strColType;
    }

    public String getStrColName() {
        return strColName;
    }

    public void setStrColName(String strColName) {
        this.strColName = strColName;
    }

    public String toString(){
        return strColName;
    }
}
