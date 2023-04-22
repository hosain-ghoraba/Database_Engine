package octree;

public class myOctPoint {

    private Comparable x;
    private Comparable y;
    private Comparable z;

    public myOctPoint(Comparable x, Comparable y, Comparable z){
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
    //overriding equals() to compare two octPoints
    public boolean equals(Object obj){
        if(obj == this){
            return true;
        }

        if(!(obj instanceof myOctPoint)){
            return false;
        }

        myOctPoint octPoint = (myOctPoint) obj;
        return octPoint.x.equals(x) && octPoint.y.equals(y) && octPoint.z.equals(z);
    }
    // overridding hashCode() to compare two octPoints when using HashMap
    public int hashCode(){
        int result = 17;
        result = 31 * result + x.hashCode();
        result = 31 * result + y.hashCode();
        result = 31 * result + z.hashCode();
        return result;
    }


    
}
