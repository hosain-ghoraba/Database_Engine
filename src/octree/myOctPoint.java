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

    
}
