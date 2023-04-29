package M2;

import java.util.HashSet;
import java.util.LinkedList;
import M1.Page;

public class Methods2 {

    public static void fillSetWithPages_satisfyingCondition_forInputValue(Octree octree, HashSet<Page> toFill, String condition, Comparable value, String axis ){ // fills the set with all whose (point) (operator) (value) == true
		
		boolean search_is_worthy = Tree_is_candidate_forSearch(octree, condition, value, axis);
		if(search_is_worthy) // if yes, then records may be found in this tree
		{
			if(octree.isLeaf())
			{
				for(OctPoint point : octree.getRecords().keySet())				
					if(conditionHolds(point , condition, value, axis)) // if the condition holds for the current point
						toFill.addAll(octree.getRecords().get(point));
			}					
			else
				for(Octree child : octree.getChildren())
					fillSetWithPages_satisfyingCondition_forInputValue(child, toFill, condition, value, axis);

		}

	}
    public static void copyAllRecordsToList(Octree octree, LinkedList<Page> toFill){ // copies all pages at all leaves of the octree to the result list
		if(octree.isLeaf())
			for(LinkedList<Page> pages : octree.getRecords().values())
				toFill.addAll(pages);	
		else
			for(Octree child : octree.getChildren())
				copyAllRecordsToList(child, toFill);

	}	
    public static boolean Tree_is_candidate_forSearch(Octree octree, String condition, Comparable value, String axis){
		if(axis.toLowerCase().equals("x"))
		{
			switch(condition)
			{
				case ">" : return octree.getRightForwardUp().getX().compareTo(value) > 0;
				case ">=" : return octree.getRightForwardUp().getX().compareTo(value) >= 0;
				case "<" : return octree.getLeftBackBottom().getX().compareTo(value) < 0;
				case "<=" : return octree.getLeftBackBottom().getX().compareTo(value) <= 0;
				case "=" : return octree.getLeftBackBottom().getX().compareTo(value) <= 0 && octree.getRightForwardUp().getX().compareTo(value) >= 0;
				case "!=" : return true;
			}

		}
		else if(axis.toLowerCase().equals("y"))
		{
			switch(condition)
			{
				case ">" : return octree.getRightForwardUp().getY().compareTo(value) > 0;
				case ">=" : return octree.getRightForwardUp().getY().compareTo(value) >= 0;
				case "<" : return octree.getLeftBackBottom().getY().compareTo(value) < 0;
				case "<=" : return octree.getLeftBackBottom().getY().compareTo(value) <= 0;
				case "=" : return octree.getLeftBackBottom().getY().compareTo(value) <= 0 && octree.getRightForwardUp().getY().compareTo(value) >= 0;
				case "!=" : return true;
			}

		}
		else if(axis.toLowerCase().equals("z"))
		{
			switch(condition)
			{
				case ">" : return octree.getRightForwardUp().getZ().compareTo(value) > 0;
				case ">=" : return octree.getRightForwardUp().getZ().compareTo(value) >= 0;
				case "<" : return octree.getLeftBackBottom().getZ().compareTo(value) < 0;
				case "<=" : return octree.getLeftBackBottom().getZ().compareTo(value) <= 0;
				case "=" : return octree.getLeftBackBottom().getZ().compareTo(value) <= 0 && octree.getRightForwardUp().getZ().compareTo(value) >= 0;
				case "!=" : return true;
			}

		}
		throw new IllegalArgumentException("Axis " + axis + " is not valid");
	}
	public static boolean conditionHolds(OctPoint recordPoint , String condition, Comparable value, String axis )
	{
		if(axis.toLowerCase().equals("x"))
		{
			switch(condition)
			{
				case ">" : return recordPoint.getX().compareTo(value) > 0;
				case ">=" : return recordPoint.getX().compareTo(value) >= 0;
				case "<" : return recordPoint.getX().compareTo(value) < 0;
				case "<=" : return recordPoint.getX().compareTo(value) <= 0;
				case "=" : return recordPoint.getX().compareTo(value) == 0;
				case "!=" : return recordPoint.getX().compareTo(value) != 0;
			}

		}
		else if(axis.toLowerCase().equals("y"))
		{
			switch(condition)
			{
				case ">" : return recordPoint.getY().compareTo(value) > 0;
				case ">=" : return recordPoint.getY().compareTo(value) >= 0;
				case "<" : return recordPoint.getY().compareTo(value) < 0;
				case "<=" : return recordPoint.getY().compareTo(value) <= 0;
				case "=" : return recordPoint.getY().compareTo(value) == 0;
				case "!=" : return recordPoint.getY().compareTo(value) != 0;
			}

		}
		else if(axis.toLowerCase().equals("z"))
		{
			switch(condition)
			{
				case ">" : return recordPoint.getZ().compareTo(value) > 0;
				case ">=" : return recordPoint.getZ().compareTo(value) >= 0;
				case "<" : return recordPoint.getZ().compareTo(value) < 0;
				case "<=" : return recordPoint.getZ().compareTo(value) <= 0;
				case "=" : return recordPoint.getZ().compareTo(value) == 0;
				case "!=" : return recordPoint.getZ().compareTo(value) != 0;
			}


		}
		throw new IllegalArgumentException("Axis " + axis + " is not valid");

	}

    public static void main(String[] args) {
        System.out.println(Integer.valueOf(2).compareTo(Integer.valueOf(1)));
    }

    
}
