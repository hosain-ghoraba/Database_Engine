package octree;

import java.math.BigInteger;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

import M1.Page;

public class myOctree {
	
	private myOctPoint leftBackDown, rightForwardUp; // the bounding box of the octree
	private myOctPoint point; // this is null if the octree is a leaf
	private myOctree[] children; // this is null if the octree is a leaf
	private HashMap<myOctPoint,ArrayList<Page> > records; // this is null if the octree is NOT a leaf

	int maxNodeCapacity = 100; // to be read from the config file 

	public myOctree(myOctPoint topLeftFront,myOctPoint bottomRightBack) { // constructor for a leaf octree, leaf octrees are converted to non-leaf octrees inside the insertIntoTree method
		this.leftBackDown = topLeftFront;
		this.rightForwardUp = bottomRightBack;
		records = new HashMap<myOctPoint, ArrayList<Page> >(maxNodeCapacity);
	}

	public void insert(Comparable x, Comparable y, Comparable z) {
	}

	public boolean isLeaf() {
		return (children == null);
	}

	public Comparable getMiddleValue(Comparable firstValue,Comparable secondValue) {
		if (firstValue instanceof Integer) {
			long middle = (0L + (Integer) firstValue + (Integer) secondValue) / 2;
			return (int) middle;
		}
		else if(firstValue instanceof Double){
			return (double)firstValue/2 + (double)secondValue/2;
		}
		else if(firstValue instanceof String){
			return medianString((String)firstValue,(String)secondValue);
		}
		else if(firstValue instanceof java.util.Date){
			BigInteger datesSum = BigInteger.ZERO;
			datesSum = datesSum.add(BigInteger.valueOf(((java.util.Date) firstValue).getTime()));
			datesSum = datesSum.add(BigInteger.valueOf(((java.util.Date) secondValue).getTime()));
			BigInteger median = datesSum.divide(BigInteger.valueOf(2));
			return new Date(median.longValue()); 
			
		}
		throw new IllegalArgumentException("Unsupported type: " + firstValue.getClass().getName());
	}

	public String medianString(String s1, String s2) {
		return null;
	}

	public static void main(String[] args) {
		Date date1 = new Date(Long.MAX_VALUE);
		Date date2 = new Date(1648540000000L);
		BigInteger datesSum = BigInteger.ZERO;
		datesSum = datesSum.add(BigInteger.valueOf(((java.util.Date) date1).getTime()));
		datesSum = datesSum.add(BigInteger.valueOf(((java.util.Date) date2).getTime()));
		BigInteger median = datesSum.divide(BigInteger.valueOf(2));
			
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		System.out.println(sdf.format(date1));
		System.out.println(sdf.format(date2));
		System.out.println(sdf.format(median));
	}

}
