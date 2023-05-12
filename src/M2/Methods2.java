package M2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import M1.DBAppException;
import M1.Page;

public class Methods2 {

    public static void fillSetWithPages_satisfyingCondition_forInputValue(Octree octree, Set<Integer> toFill, String x_condition, String y_condition, String z_condition, Comparable x_value, Comparable y_value, Comparable z_value ){ // fills the set with all points where (point) (operator) (value) == true
		
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
	public static Comparable parseType(String val, String dataType) throws DBAppException {
		try {
			if (dataType.equals("java.lang.Integer")) {
				return Integer.parseInt(val);
			}
			if (dataType.equals("java.lang.Double")) {
				return Double.parseDouble(val);
			}
			if (dataType.equals("java.util.Date")) {
				return new SimpleDateFormat("yyyy/MM/dd").parse(val);
			}
			return val;
		} catch (ParseException i) {
			throw new DBAppException("Cannot parse value to passed type");
		}
	}
	public static List<List<Object>> getSubsetsOfSizeK(List<Object> input, int k) {
		List<List<Object>> result = new LinkedList<>();
		if (input == null || input.size() < k) {
			return result;
		}

		int n = input.size();
		int[] index = new int[k];
		for (int i = 0; i < k; i++) {
			index[i] = i;
		}

		while (index[0] <= n - k) {
			List<Object> subset = new ArrayList<>(k);
			for (int i = 0; i < k; i++) {
				subset.add(input.get(index[i]));
			}
			result.add(subset);

			int t = k - 1;
			while (t != 0 && index[t] == n - k + t) {
				t--;
			}
			index[t]++;
			for (int i = t + 1; i < k; i++) {
				index[i] = index[i - 1] + 1;
			}
		}
		return result;
	}
	public static List<String> getAllPermutations(String colName1,String colName2, String colName3){

		List<String> result = new LinkedList<>();
		result.add(colName1+colName2+colName3);
		result.add(colName1+colName3+colName2);
		result.add(colName2+colName1+colName3);
		result.add(colName2+colName3+colName1);
		result.add(colName3+colName1+colName2);
		result.add(colName3+colName2+colName1);
		return result;
	}


	// drafts 
	
	// private HashSet<Integer> selectPages_UsingIndex(SQLTerm[] arrSQLTerms, String[] strarrOperators) {
    //     List<Object> comparessedParamerters = compact(arrSQLTerms, strarrOperators);
    //     List param1 = (List)comparessedParamerters.get(0);
    //     List param2 = (List)comparessedParamerters.get(1);
    //     LinkedList<Object>[] pages = new LinkedList[param1.size()];
    //     String[] operators = new String[param2.size()];
    //     for (int i = 0; i < param1.size(); i++) 
    //         pages[i] = (LinkedList<Object>) param1.get(i);
    //     for (int i = 0; i < param2.size(); i++) 
    //         operators[i] = (String) param2.get(i);
    //     LinkedList<Object> obj_pagesID = applyOperatorsFromLeftToRight(pages, strarrOperators);
    //     HashSet<Integer> pagesID = new HashSet<Integer>();
    //     for (Object page : obj_pagesID)        
    //         pagesID.add((Integer) page);      
    //     return pagesID;
        
    // }
	// public boolean indexCanHelp(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws IOException, DBAppException {
    //     List<int[]> ANDChains = getAndedTermsBoundries(Arrays.asList(strarrOperators));
    //     for(int[] chainBoundry : ANDChains)
    //     {
    //         if(chainBoundry[1] - chainBoundry[0] >= 2)
    //         {
    //             List<Object> chain = new LinkedList<>();
    //             for(int i = chainBoundry[0] ; i < chainBoundry[1] ; i++)
    //                 chain.add(arrSQLTerms[i]);
    //             List<List<Object>> allTriples = Methods2.getSubsetsOfSizeK(chain,3);
    //             for(List<Object> triple : allTriples)
    //                 if(termsFormIndex((SQLTerm) triple.get(0), (SQLTerm) triple.get(1), (SQLTerm) triple.get(2), arrSQLTerms[0]._strTableName))
    //                     return true;         
    //         }
            
    //     }
    //     return false;
        
    // }
	// public static List<int[]> getAndedTermsBoundries(List<String> operators){
	// 	List<int[]> result = new LinkedList<>();
	// 	for(int i = 0; i < operators.size(); i++)
	// 		if(operators.get(i).equals("AND"))	
	// 		{
	// 			int j;
	// 			for(j = i+1; j < operators.size(); j++)
	// 			{
	// 				if(operators.get(j).equals("AND"))
	// 					continue;
	// 				else
	// 					break;
	// 			}
	// 			result.add(new int[]{i, j});// i is the index of the sql operator before the first AND, j is the index of the sql operator after the last AND		
	// 			i = j;
		
	// 		}
	// 	return result;


	// } 
	// public List<Object> compact(SQLTerm[] arrSQLTerms, String[] strarrOperators){// to complete later
    //     // convert inputs to lists
    //     List<SQLTerm> termsList = new LinkedList<SQLTerm>();
    //     List<String> operatorsList = new LinkedList<String>();
    //     for(SQLTerm term : arrSQLTerms)
    //         termsList.add(term);
    //     for(String operator : strarrOperators)
    //         operatorsList.add(operator);
    //     return null; // to remove later    


    // }
	
	public static void main(String[] args) {

		Object o = Integer.valueOf(0);
		System.out.println(o.getClass().getName());

    }

    
}
