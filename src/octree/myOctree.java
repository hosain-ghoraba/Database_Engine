package octree;

import java.util.ArrayList;
import java.util.HashMap;
import M1.Page;

public class myOctree {
	
	private myOctPoint leftBackBottom; // this is edge number 0 in the "edge numbers" photo in github 
	private myOctPoint rightForwardUp; // the bounding box of the octree
	private myOctPoint point; // this is null if the octree is a leaf
	private myOctree[] children; // this is null if the octree is a leaf
	private HashMap<myOctPoint,ArrayList<Page> > records; // this is null if the octree is NOT a leaf
	int maxNodeCapacity = 100; // to be read from the config file 

	public myOctree(myOctPoint leftBackBottom,myOctPoint rightForwardUp) { // constructor for a leaf octree, leaf octrees are converted to non-leaf octrees by the split method when their capacity is exceeded
		this.leftBackBottom = leftBackBottom;
		this.rightForwardUp = rightForwardUp;
		records = new HashMap<myOctPoint, ArrayList<Page> >(maxNodeCapacity);
	}

	public void insertPageIntoTree(Comparable x, Comparable y, Comparable z , Page page) {

		boolean xNotInRange = (x.compareTo(leftBackBottom.getX()) < 0 || x.compareTo(rightForwardUp.getX()) > 0);
		boolean yNotInRange = (y.compareTo(leftBackBottom.getY()) < 0 || y.compareTo(rightForwardUp.getY()) > 0);
		boolean zNotInRange = (z.compareTo(leftBackBottom.getZ()) < 0 || z.compareTo(rightForwardUp.getZ()) > 0);
		if(xNotInRange || yNotInRange || zNotInRange) 
			throw new IllegalArgumentException("Point " + x + " " + y + " " + z + " is not in the range of the octree");

		if(! this.isLeaf())
		{
			int position = myCollection.getRelevantPosition(this.point.getX(), this.point.getY(), this.point.getZ(), x, y, z);
			children[position].insertPageIntoTree(x, y, z , page);
		}	
		else
		{
			myOctPoint PointToInsert = new myOctPoint(x,y,z);
			if(!records.containsKey(PointToInsert))
			{
				ArrayList<Page> pages = new ArrayList<Page>();
				pages.add(page);
				records.put(PointToInsert, pages);
			}
			else
			{
				records.get(PointToInsert).add(page);
			}
			
			if(records.size() > maxNodeCapacity)			
				this.split();
			
		}
	}
	private void split() {
		
		Comparable Xsmall = this.leftBackBottom.getX();
		Comparable Ysmall = this.leftBackBottom.getY();
		Comparable Zsmall = this.leftBackBottom.getZ();
		
		Comparable Xbig = this.rightForwardUp.getX();
		Comparable Ybig = this.rightForwardUp.getY();
		Comparable Zbig = this.rightForwardUp.getZ();
		
		Comparable xCenter = myCollection.getMiddleValue(Xsmall, Xbig);
		Comparable yCenter = myCollection.getMiddleValue(Ysmall, Ybig);
		Comparable zCenter = myCollection.getMiddleValue(Zsmall, Zbig);

		this.point = new myOctPoint(xCenter,yCenter,zCenter);
		children = new myOctree[8];

		children[0] = new myOctree(new myOctPoint(Xsmall, Ysmall, Zsmall), new myOctPoint(xCenter,yCenter,zCenter));
		children[1] = new myOctree(new myOctPoint(Xsmall,Ysmall,zCenter), new myOctPoint(xCenter,yCenter,Zbig));
		children[2] = new myOctree(new myOctPoint(Xsmall,yCenter,Zsmall), new myOctPoint(xCenter,Ybig,zCenter));
		children[3] = new myOctree(new myOctPoint(Xsmall,yCenter,zCenter), new myOctPoint(xCenter,Ybig,Zbig));	
		children[4] = new myOctree(new myOctPoint(xCenter,Ysmall,Zsmall), new myOctPoint(Xbig,yCenter,zCenter));
		children[5] = new myOctree(new myOctPoint(xCenter,Ysmall,zCenter), new myOctPoint(Xbig,yCenter,Zbig));
		children[6] = new myOctree(new myOctPoint(xCenter,yCenter,Zsmall), new myOctPoint(Xbig,Ybig,zCenter));
		children[7] = new myOctree(new myOctPoint(xCenter,yCenter,zCenter), new myOctPoint(Xbig,Ybig,Zbig));

		for(myOctPoint recordPoint : records.keySet()) // transfer the records to the children
		{
			ArrayList<Page> recordPages = records.get(recordPoint);
			int childPositionToInsertInto = myCollection.getRelevantPosition(this.point.getX(), this.point.getY(), this.point.getZ(), recordPoint.getX(), recordPoint.getY(), recordPoint.getZ());
			for(Page page : recordPages)
			{				
				children[childPositionToInsertInto].insertPageIntoTree(recordPoint.getX(), recordPoint.getY(), recordPoint.getZ(), page);
			}			
		}
		records = null; // after all the records are transferred to the children, the records are no longer needed in the parent

	}
	private boolean isLeaf() {
		return (children == null);
	}

    public static void main(String[] args) {
		
		HashMap<myOctPoint,String> records = new HashMap<myOctPoint,String>();
		records.put(new myOctPoint(1,1,1), "test");
		System.out.println(records.get(new myOctPoint(1,1,1)));
	    
    }
}

	
	
	
	

