package M2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import M1.Page;

public class Methods2 {

    public static void fillSetWithPages_satisfyingCondition_forInputValue(Octree octree, HashSet<Integer> toFill, String x_condition, String y_condition, String z_condition, Comparable x_value, Comparable y_value, Comparable z_value ){ // fills the set with all points where (point) (operator) (value) == true
		
		boolean search_is_worthy = Tree_is_candidate_forSearch(octree, x_condition, y_condition, z_condition, x_value, y_value, z_value);
		if(search_is_worthy) 
		{
			if(octree.isLeaf())
			{
				for(OctPoint point : octree.getRecords().keySet())				
					if(conditionsHold(point, x_condition, y_condition, z_condition, x_value, y_value, z_value)) 
						toFill.addAll(octree.getRecords().get(point));
			}					
			else
				for(Octree child : octree.getChildren())
					fillSetWithPages_satisfyingCondition_forInputValue(child, toFill, x_condition, y_condition, z_condition, x_value, y_value, z_value);

		}

	}   
    public static boolean Tree_is_candidate_forSearch(Octree octree, String x_operator, String y_operator, String z_operator, Comparable x_value, Comparable y_value, Comparable z_value){ // checks if the octree is a candidate for the search (if it's not, then we don't search it)
		
		boolean x_dimention_is_candidate = false;
		boolean y_dimention_is_candidate = false;
		boolean z_dimention_is_candidate = false;

		if(x_operator == null)
			x_dimention_is_candidate = true;
		else
		{
			switch(x_operator)
				{
					case ">" : x_dimention_is_candidate = octree.getRightForwardUp().getX().compareTo(x_value) > 0; break;
					case ">=" : x_dimention_is_candidate = octree.getRightForwardUp().getX().compareTo(x_value) >= 0; break;
					case "<" : x_dimention_is_candidate = octree.getLeftBackBottom().getX().compareTo(x_value) < 0; break;
					case "<=" : x_dimention_is_candidate = octree.getLeftBackBottom().getX().compareTo(x_value) <= 0; break;
					case "=" : x_dimention_is_candidate = octree.getLeftBackBottom().getX().compareTo(x_value) <= 0 && octree.getRightForwardUp().getX().compareTo(x_value) >= 0; break;
					case "!=" : x_dimention_is_candidate = true; break;
				}

		}
		if(y_operator == null)
			y_dimention_is_candidate = true;
		else
		{
			switch(y_operator)
				{
					case ">" : y_dimention_is_candidate = octree.getRightForwardUp().getY().compareTo(y_value) > 0; break;
					case ">=" : y_dimention_is_candidate = octree.getRightForwardUp().getY().compareTo(y_value) >= 0; break;
					case "<" : y_dimention_is_candidate = octree.getLeftBackBottom().getY().compareTo(y_value) < 0; break;
					case "<=" : y_dimention_is_candidate = octree.getLeftBackBottom().getY().compareTo(y_value) <= 0; break;
					case "=" : y_dimention_is_candidate = octree.getLeftBackBottom().getY().compareTo(y_value) <= 0 && octree.getRightForwardUp().getY().compareTo(y_value) >= 0; break;
					case "!=" : y_dimention_is_candidate = true; break;
				}
			
		}
		if(z_operator == null)
			z_dimention_is_candidate = true;
		else
		{
			switch(z_operator)
				{
					case ">" : z_dimention_is_candidate = octree.getRightForwardUp().getZ().compareTo(z_value) > 0; break;
					case ">=" : z_dimention_is_candidate = octree.getRightForwardUp().getZ().compareTo(z_value) >= 0; break;
					case "<" : z_dimention_is_candidate = octree.getLeftBackBottom().getZ().compareTo(z_value) < 0; break;
					case "<=" : z_dimention_is_candidate = octree.getLeftBackBottom().getZ().compareTo(z_value) <= 0; break;
					case "=" : z_dimention_is_candidate = octree.getLeftBackBottom().getZ().compareTo(z_value) <= 0 && octree.getRightForwardUp().getZ().compareTo(z_value) >= 0; break;
					case "!=" : z_dimention_is_candidate = true; break;
				}
			
		}
		return x_dimention_is_candidate && y_dimention_is_candidate && z_dimention_is_candidate;
	}	
	public static boolean conditionsHold(OctPoint recordPoint , String x_operator, String y_operator, String z_operator , Comparable x_value, Comparable y_value, Comparable z_value) // checks if the condition holds for the recordPoint
	{
		boolean x_condition_holds = false;
		boolean y_condition_holds = false;
		boolean z_condition_holds = false;

		if(x_operator == null)
			x_condition_holds = true;
		else
		{
			switch(x_operator)
				{
					case ">" : x_condition_holds = recordPoint.getX().compareTo(x_value) > 0; break;
					case ">=" : x_condition_holds = recordPoint.getX().compareTo(x_value) >= 0; break;
					case "<" : x_condition_holds = recordPoint.getX().compareTo(x_value) < 0; break;
					case "<=" : x_condition_holds = recordPoint.getX().compareTo(x_value) <= 0; break;
					case "=" : x_condition_holds = recordPoint.getX().compareTo(x_value) == 0; break;
					case "!=" : x_condition_holds = recordPoint.getX().compareTo(x_value) != 0; break;
				}

		}	
		if(y_operator == null)
			y_condition_holds = true;
		else
		{
			switch(y_operator)
				{
					case ">" : y_condition_holds = recordPoint.getY().compareTo(y_value) > 0; break;
					case ">=" : y_condition_holds = recordPoint.getY().compareTo(y_value) >= 0; break;
					case "<" : y_condition_holds = recordPoint.getY().compareTo(y_value) < 0; break;
					case "<=" : y_condition_holds = recordPoint.getY().compareTo(y_value) <= 0; break;
					case "=" : y_condition_holds = recordPoint.getY().compareTo(y_value) == 0; break;
					case "!=" : y_condition_holds = recordPoint.getY().compareTo(y_value) != 0; break;
				}
			
		}
		if(z_operator == null)
			z_condition_holds = true;
		else
		{
			switch(z_operator)
				{
					case ">" : z_condition_holds = recordPoint.getZ().compareTo(z_value) > 0; break;
					case ">=" : z_condition_holds = recordPoint.getZ().compareTo(z_value) >= 0; break;
					case "<" : z_condition_holds = recordPoint.getZ().compareTo(z_value) < 0; break;
					case "<=" : z_condition_holds = recordPoint.getZ().compareTo(z_value) <= 0; break;
					case "=" : z_condition_holds = recordPoint.getZ().compareTo(z_value) == 0; break;
					case "!=" : z_condition_holds = recordPoint.getZ().compareTo(z_value) != 0; break;
				}
			
		}
		
		return x_condition_holds && y_condition_holds && z_condition_holds;	
	}
	public static void copyAllRecordsToList(Octree octree, LinkedList<Integer> toFill){ // copies all pages at all leaves of the octree to the result list
		if(octree.isLeaf())
			for(LinkedList<Integer> pages : octree.getRecords().values())
				toFill.addAll(pages);	
		else
			for(Octree child : octree.getChildren())
				copyAllRecordsToList(child, toFill);

	}	
    
	public static void main(String[] args) {

		Object o = Integer.valueOf(0);
		System.out.println(o.getClass().getName());

    }

    
}
