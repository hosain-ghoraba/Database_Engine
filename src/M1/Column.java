package M1;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

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
    // added in M2
    public Comparable getMinValue() throws ParseException, DBAppException {
        
        switch(strColType)
        {
            case "java.lang.Integer" : return Integer.parseInt(strMinVal);
            case "java.lang.Double" : return Double.parseDouble(strMinVal);
            case "java.lang.String" : return strMinVal;
            case "java.util.Date" : return new SimpleDateFormat("yyyy-MM-dd").parse(strMinVal);
            default : throw new DBAppException("column type not supported!");
        }       
    }
    public Comparable getMaxValue() throws ParseException, DBAppException {
        
        switch(strColType)
        {
            case "java.lang.Integer" : return Integer.parseInt(strMaxVal);
            case "java.lang.Double" : return Double.parseDouble(strMaxVal);
            case "java.lang.String" : return strMaxVal;
            case "java.util.Date" : return new SimpleDateFormat("yyyy-MM-dd").parse(strMaxVal);
            default : throw new DBAppException("column type not supported!");
        }       
    }
}
