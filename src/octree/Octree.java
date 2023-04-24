package octree;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Vector;

import M1.DBApp;
import M1.DBAppException;
import M1.Page;
import M1.Row;

public class Octree {
	
	private int size; // sum of records in all leaves
	private OctPoint leftBackBottom; // this is edge number 0 in the "edge numbers" photo in github 
	private OctPoint rightForwardUp; // the bounding box of the octree
	private OctPoint point; // this is null if the octree is a leaf
	private Octree[] children; // this is null if the octree is a leaf
	private HashMap<OctPoint,LinkedList<Page> > records; // this is null if the octree is NOT a leaf
	int maxNodeCapacity = 2; // to be read from the config file 

	//// below are the methods that will be used by the DBApp class ////
	
	public Octree(OctPoint leftBackBottom,OctPoint rightForwardUp) { // constructor for a leaf octree, leaf octrees are converted to non-leaf octrees by the split method when their capacity is exceeded
		size = 0;
		this.leftBackBottom = leftBackBottom;
		this.rightForwardUp = rightForwardUp;
		records = new HashMap<OctPoint, LinkedList<Page> >();
	}
	public void insertPageIntoTree(Comparable x, Comparable y, Comparable z , Page page) {
		this.insertHelper(x, y, z , page, new LinkedList<Octree>());
	}
	public void deletePageFromTree(Comparable x, Comparable y, Comparable z , Page page) { // deletes ONLY one instance of the page, in case multiple instances of the same page exist in the same point
		this.deleteHelper(x, y, z , page, new LinkedList<Octree>());
	}
	public boolean pointExists(Comparable x, Comparable y, Comparable z){
		validatePointIsInTreesBounday(x, y, z);
		if(this.isLeaf())
			return records.containsKey(new OctPoint(x,y,z));
		else
		{
			int position = myCollection.getRelevantPosition(this.point.getX(), this.point.getY(), this.point.getZ(), x, y, z);
			return children[position].pointExists(x, y, z);
		}

	}	
	public LinkedList<Page> getPagesAtPoint(Comparable x, Comparable y, Comparable z) {
		if(!this.pointExists(x, y, z))
			throw new IllegalArgumentException("Point " + x + " " + y + " " + z + " doesn't exist in the octree");
		if(this.isLeaf())
			return records.get(new OctPoint(x,y,z));
		else
		{
			int position = myCollection.getRelevantPosition(this.point.getX(), this.point.getY(), this.point.getZ(), x, y, z);
			return children[position].getPagesAtPoint(x, y, z);
		}
	} 
	public void replacePageAtPoint(Comparable x, Comparable y, Comparable z , Page oldPage, Page newPage) {// replaces ONLY one instance of the page, in case multiple instances of the same page exist in the same point
		if(!this.pointExists(x, y, z))
			throw new IllegalArgumentException("Point " + x + " " + y + " " + z + " doesn't exist in the octree");
		deletePageFromTree(x, y, z, oldPage);
		insertPageIntoTree(x, y, z, newPage);
	}
	public void printTree() {
		printTreeHelper(0);
	}
	
	//// below are helper private methods ////
	private void insertHelper(Comparable x, Comparable y, Comparable z , Page page , LinkedList<Octree> traversedSoFar) {

		validatePointIsInTreesBounday(x, y, z);		
		traversedSoFar.add(this);
		if(! this.isLeaf())
		{
			int position = myCollection.getRelevantPosition(this.point.getX(), this.point.getY(), this.point.getZ(), x, y, z);
			children[position].insertHelper(x, y, z , page, traversedSoFar);
		}	
		else
		{
			OctPoint PointToInsert = new OctPoint(x,y,z);
			if(!records.containsKey(PointToInsert))
			{
				LinkedList<Page> pages = new LinkedList<Page>();
				pages.add(page);
				records.put(PointToInsert, pages);
				for(Octree octree : traversedSoFar)
					octree.size++; 
			}
			else
			{
				records.get(PointToInsert).add(page);
				// no need to increment size because we now inserted a duplicate page,which is not counted in the size
			}
			
			if(records.size() > maxNodeCapacity)			
				this.split();
			
		}
	}
	private void deleteHelper(Comparable x, Comparable y, Comparable z , Page page, LinkedList<Octree> traversedSoFar) {

		if(!this.pointExists(x, y, z))
			throw new IllegalArgumentException("PointToDelete " + x + " " + y + " " + z + " doesn't exist in the octree");
		traversedSoFar.addLast(this);
		if(! this.isLeaf())
		{
			int position = myCollection.getRelevantPosition(this.point.getX(), this.point.getY(), this.point.getZ(), x, y, z);
			children[position].deleteHelper(x, y, z , page, traversedSoFar);
		}	
		else
		{
			OctPoint PointToDelete = new OctPoint(x,y,z);
			LinkedList<Page> pages = records.get(PointToDelete);
			boolean succefullyDeleted = pages.remove(page); // remove page from list of pages
			if(!succefullyDeleted)
				throw new IllegalArgumentException("the input page " + page + "  was not found in the list of pages of Point " + x + " " + y + " " + z + "");	
			if(pages.size() == 0)  // if the list became empty, remove the point with its empty list from the records
			{
				records.remove(PointToDelete);
				for(Octree octree : traversedSoFar)
					octree.size--;
				// merge all leaves with parent if parent size became not more than maxNodeCapacity
				traversedSoFar.removeLast(); // remove the current leaf from the list of parents	
				while(!traversedSoFar.isEmpty() )
				{
					Octree directParent = traversedSoFar.removeLast();
					if(directParent.size <= maxNodeCapacity)						
						directParent.devourChildren();							
					else
						break; // if the direct parent size is still more than maxNodeCapacity,then so are all other non-direct parents
				}							 
			}
		
		}
	}
	private void devourChildren() {
		if(this.isLeaf())
			return;
		this.records = new HashMap<OctPoint, LinkedList<Page> >(); // preparing a non-leaf octree to be a leaf octree	
		for(Octree child : this.children)
		{
			child.devourChildren();
			for(OctPoint key : child.records.keySet())
			{
				LinkedList<Page> value = child.records.get(key);
				this.records.put(key, value);
			}
		}
		this.children = null;
		this.point = null;
		
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

		this.point = new OctPoint(xCenter,yCenter,zCenter);
		children = new Octree[8];

		children[0] = new Octree(new OctPoint(Xsmall, Ysmall, Zsmall), new OctPoint(xCenter,yCenter,zCenter));
		children[1] = new Octree(new OctPoint(Xsmall,Ysmall,zCenter), new OctPoint(xCenter,yCenter,Zbig));
		children[2] = new Octree(new OctPoint(Xsmall,yCenter,Zsmall), new OctPoint(xCenter,Ybig,zCenter));
		children[3] = new Octree(new OctPoint(Xsmall,yCenter,zCenter), new OctPoint(xCenter,Ybig,Zbig));	
		children[4] = new Octree(new OctPoint(xCenter,Ysmall,Zsmall), new OctPoint(Xbig,yCenter,zCenter));
		children[5] = new Octree(new OctPoint(xCenter,Ysmall,zCenter), new OctPoint(Xbig,yCenter,Zbig));
		children[6] = new Octree(new OctPoint(xCenter,yCenter,Zsmall), new OctPoint(Xbig,Ybig,zCenter));
		children[7] = new Octree(new OctPoint(xCenter,yCenter,zCenter), new OctPoint(Xbig,Ybig,Zbig));

		for(OctPoint recordPoint : records.keySet()) // transfer the records to the children
		{
			LinkedList<Page> recordPages = records.get(recordPoint);
			int childPositionToInsertInto = myCollection.getRelevantPosition(this.point.getX(), this.point.getY(), this.point.getZ(), recordPoint.getX(), recordPoint.getY(), recordPoint.getZ());
			for(Page page : recordPages)
			{				
				LinkedList<Octree> traversedSoFar = new LinkedList<Octree>();
				children[childPositionToInsertInto].insertHelper(recordPoint.getX(), recordPoint.getY(), recordPoint.getZ(), page, traversedSoFar);
			}			
		}
		records = null; // after all the records are transferred to the children, the records are no longer needed in the parent

	}
	private boolean isLeaf() {
		return (children == null);
	}	
	private void  validatePointIsInTreesBounday(Comparable x, Comparable y, Comparable z) {
		boolean xIsInBoundary = (x.compareTo(this.leftBackBottom.getX()) >= 0) && (x.compareTo(this.rightForwardUp.getX()) <= 0);
		boolean yIsInBoundary = (y.compareTo(this.leftBackBottom.getY()) >= 0) && (y.compareTo(this.rightForwardUp.getY()) <= 0);
		boolean zIsInBoundary = (z.compareTo(this.leftBackBottom.getZ()) >= 0) && (z.compareTo(this.rightForwardUp.getZ()) <= 0);
		if(!xIsInBoundary || !yIsInBoundary || !zIsInBoundary)
			throw new IllegalArgumentException("Point " + x + " " + y + " " + z + " can't exist in the tree because it is not even between " + leftBackBottom + " and " + rightForwardUp + " !");
	}	
	private void printTreeHelper(int curLevel) { // to visualize the output, each level has a unique indentation before it

		String levelSpaces = createSpaces(curLevel*5);
		if(this.isLeaf())
		{
			if(this.size > 0)
			{
				System.out.print(levelSpaces + "level " + curLevel + " Leaf " + this.leftBackBottom + " " + this.rightForwardUp + " " + "no.records : " + this.size + " " );
				System.out.print(createSpaces(curLevel+1));
				for(OctPoint key : this.records.keySet())	
					System.out.print("key : " + key + " " + "list size : " + this.records.get(key).size() + "    ,");
				System.out.println();	
			}
		}
		else
		{
			System.out.println(levelSpaces + "level " + curLevel + " Non-leaf " + this.leftBackBottom + " " + this.rightForwardUp + " size " + this.size);
			for(int i = 0 ; i < children.length ; i++)
			{
				children[i].printTreeHelper(curLevel+1); 
			}
		}

	}
	private String createSpaces(int spaces){
		StringBuilder sb = new StringBuilder();
		for(int i = 0 ; i < spaces ; i++)
			sb.append(" ");
		return sb.toString();

	}
    public static void main(String[] args) throws DBAppException {

		DBApp d = new DBApp();
		Hashtable<String, String> htNameType = new Hashtable<>();
		htNameType.put("Id", "java.lang.Integer");
		htNameType.put("Name", "java.lang.String");
		htNameType.put("Job", "java.lang.String");
		Hashtable<String, String> htNameMin = new Hashtable<>();
		htNameMin.put("Id", "1");
		htNameMin.put("Name", "AAA");
		htNameMin.put("Job", "blacksmith");
		Hashtable<String, String> htNameMax = new Hashtable<>();
		htNameMax.put("Id", "1000");
		htNameMax.put("Name", "zaky");
		htNameMax.put("Job", "zzz");

		d.createTable("University", "Id", htNameType, htNameMin, htNameMax);
		Hashtable<String, Object> htColNameVal0 = new Hashtable<>();
		htColNameVal0.put("Id", 23);
		htColNameVal0.put("Name", new String("ahmed"));
		htColNameVal0.put("Job", new String("blacksmith"));

		Hashtable<String, Object> htColNameVal1 = new Hashtable<>();
		htColNameVal1.put("Id", 33);
		htColNameVal1.put("Name", new String("ali"));
		htColNameVal1.put("Job", new String("engineer"));

		Hashtable<String, Object> htColNameVal2 = new Hashtable<>();
		htColNameVal2.put("Id", 11);
		htColNameVal2.put("Name", new String("dani"));
		htColNameVal2.put("Job", new String("doctor"));

		Hashtable<String, Object> htColNameVal3 = new Hashtable<>();
		htColNameVal3.put("Id", 15);
		htColNameVal3.put("Name", new String("basem"));
		htColNameVal3.put("Job", new String("teacher"));

		Hashtable<String, Object> htColNameVal4 = new Hashtable<>();
		htColNameVal4.put("Id", 14);
		htColNameVal4.put("Name", new String("mostafa"));
		htColNameVal4.put("Job", new String("engineer"));

		
		Page page1 = new Page("University", 1);
		Page page2 = new Page("University", 2);
		Page page3 = new Page("University", 3);
		Page page4 = new Page("University", 4);
		Page page5 = new Page("University", 5);
		Page page6 = new Page("University", 6);
		Page page7 = new Page("University", 7);
		Page page8 = new Page("University", 8);
		Page page9 = new Page("University", 9);
		Page page10 = new Page("University", 10);

		page1.insertAnEntry(new Row(new Vector<Object>(htColNameVal0.values())));
		page1.insertAnEntry(new Row(new Vector<Object>(htColNameVal0.values())));
		page1.insertAnEntry(new Row(new Vector<Object>(htColNameVal0.values())));
		page2.insertAnEntry(new Row(new Vector<Object>(htColNameVal1.values())));
		page2.insertAnEntry(new Row(new Vector<Object>(htColNameVal1.values())));
		page3.insertAnEntry(new Row(new Vector<Object>(htColNameVal2.values())));
		page4.insertAnEntry(new Row(new Vector<Object>(htColNameVal3.values())));
		page5.insertAnEntry(new Row(new Vector<Object>(htColNameVal4.values())));
		page6.insertAnEntry(new Row(new Vector<Object>(htColNameVal0.values())));

		

		Octree octree = new Octree(new OctPoint(0,0,0), new OctPoint(100,100,100));

		octree.insertPageIntoTree(10, 10, 10, page1);
		octree.insertPageIntoTree(15, 15, 15, page1);
		octree.insertPageIntoTree(20, 20, 20, page2);
		octree.insertPageIntoTree(25, 25, 25, page3);
		octree.insertPageIntoTree(30, 30, 30, page4);
		octree.insertPageIntoTree(35, 35, 35, page5);
		octree.insertPageIntoTree(40, 40, 40, page6);
		octree.insertPageIntoTree(45, 45, 45, page7);
		octree.insertPageIntoTree(50, 50, 50, page8);
		octree.insertPageIntoTree(55, 55, 55, page9);
		octree.insertPageIntoTree(60, 60, 60, page10);
		octree.insertPageIntoTree(65, 65, 65, page1);
		octree.insertPageIntoTree(70, 70, 70, page2);
		octree.insertPageIntoTree(75, 75, 75, page3);
		octree.insertPageIntoTree(80, 80, 80, page4);
		octree.insertPageIntoTree(85, 85, 85, page5);
		octree.insertPageIntoTree(90, 90, 90, page6);
		octree.insertPageIntoTree(95, 95, 95, page7);
		octree.insertPageIntoTree(100, 100, 100, page8);

		
		octree.deletePageFromTree(15, 15, 15, page1);
		octree.deletePageFromTree(20, 20, 20, page2);
		octree.deletePageFromTree(25, 25, 25, page3);
		octree.deletePageFromTree(30, 30, 30, page4);
		octree.deletePageFromTree(35, 35, 35, page5);
		octree.deletePageFromTree(40, 40, 40, page6);
		octree.deletePageFromTree(45, 45, 45, page7);
		octree.deletePageFromTree(50, 50, 50, page8);
		octree.deletePageFromTree(55, 55, 55, page9);
		// octree.deletePageFromTree(60, 60, 60, page10);
		// octree.deletePageFromTree(65, 65, 65, page1);
		// octree.deletePageFromTree(70, 70, 70, page2);
		// octree.deletePageFromTree(75, 75, 75, page3);
		// octree.deletePageFromTree(80, 80, 80, page4);
		// octree.deletePageFromTree(85, 85, 85, page5);
		// octree.deletePageFromTree(90, 90, 90, page6);
		// octree.deletePageFromTree(95, 95, 95, page7);
		// octree.deletePageFromTree(100, 100, 100, page8);

		octree.printTree();
	    
    }
}

	
	
	
	

