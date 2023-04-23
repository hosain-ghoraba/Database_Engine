package octree;

public class OctPoint {

    private Comparable x;
    private Comparable y;
    private Comparable z;

    public OctPoint(Comparable x, Comparable y, Comparable z){
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public Comparable getX(){
        return x;
    }
    public Comparable getY(){
        return y;
    }
    public Comparable getZ(){
        return z;
    }
    
    public boolean equals(Object obj){ //overriding equals() to check equality of two octPoints using their x,y,z values not their references
        if(obj == this){
            return true;
        }

        if(!(obj instanceof OctPoint)){
            return false;
        }

        OctPoint octPoint = (OctPoint) obj;
        return octPoint.x.equals(x) && octPoint.y.equals(y) && octPoint.z.equals(z);
    }  
    public int hashCode(){ // overridding hashCode() to compare two octPoints when using HashMap by their x,y,z values not their references
        int result = 17;
        result = 31 * result + x.hashCode();
        result = 31 * result + y.hashCode();
        result = 31 * result + z.hashCode();
        return result;
    }
    public String toString(){
        return "(" + x + "," + y + "," + z + ")";
    }


    
}
