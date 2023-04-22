package octree;

import java.math.BigInteger;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;

import M1.Page;

public class myCollection { // a collection of static methods that are used in the myOctree class

    // doesn't return a value EXACTLY in the middle of the two strings, 
    //but approximately in the middle (can't do better than this!) (and the doctor said that in piazza anyway)
    
    public static String medianString(String s1,String s2){ 
        
        StringBuilder sb1 = new StringBuilder(s1);
        StringBuilder sb2 = new StringBuilder(s2);
       
        while(sb1.length() < sb2.length())
            sb1.append("a");
        while(sb1.length() > sb2.length())
            sb2.append("a");
        s1 = sb1.toString();
        s2 = sb2.toString();
        String smallerString = s1.compareTo(s2) < 0 ? s1 : s2;
        String biggerString = smallerString.equals(s1) ? s2 : s1;
			
        return myCollection.medianFromGeeksForGeeks(smallerString, biggerString);

    }
    public static String medianFromGeeksForGeeks(String s1, String s2)
    {
        if(s1.length() != s2.length())
            throw new IllegalArgumentException("Strings must be of the same length");
        int N = s1.length();
        int[] a1 = new int[N + 1];
        for (int i = 0; i < N; i++) 
            a1[i + 1] = (int)s1.charAt(i) - 97 + (int)s2.charAt(i) - 97;
        for (int i = N; i >= 1; i--)
        {
            a1[i - 1] += (int)a1[i] / 26;
            a1[i] %= 26;
        }

        for (int i = 0; i <= N; i++) 
        {
            if ((a1[i] & 1) != 0) 
                if (i + 1 <= N) 
                    a1[i + 1] += 26;
            a1[i] = (int)a1[i] / 2;
        }
        StringBuilder result = new StringBuilder();
        for (int i = 1; i <= N; i++) 
            result.append((char)(a1[i] + 97));
        return result.toString();
    }
    public static Comparable getMiddleValue(Comparable firstValue,Comparable secondValue) {
		if(!(firstValue.getClass().equals(secondValue.getClass()))) 
			throw new IllegalArgumentException("Values must be of the same type");
		
		if (firstValue instanceof Integer) {
			long middle = (0L + (Integer) firstValue + (Integer) secondValue) / 2;
			return (int) middle;
		}
		else if(firstValue instanceof Double){
			return (double)firstValue/2 + (double)secondValue/2;
		}
		else if(firstValue instanceof String){
			return myCollection.medianString((String)firstValue, (String)secondValue);
			
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
    public static int getRelevantPosition(Comparable x, Comparable y, Comparable z, Comparable x2, Comparable y2, Comparable z2) {
        // relevant position of the point (x2,y2,z2) with respect to the point (x,y,z) 
        // considering that (x,y,z) is the center of a cube, and (x2,y2,z2) is an edge of the cube
        // look photo attached to the project with name "relevant positions" to understand better

        int x_RelevantPosition = x.compareTo(x2) >= 0 ? 1 : 0;
        int y_RelevantPosition = y.compareTo(y2) >= 0 ? 1 : 0;
        int z_RelevantPosition = z.compareTo(z2) >= 0 ? 1 : 0;
        String direction = "" + x_RelevantPosition + y_RelevantPosition + z_RelevantPosition;
        switch (direction) {
            case "000":
                return 0;
            case "001":
                return 1;
            case "010":
                return 2;
            case "011":
                return 3;
            case "100":
                return 4;
            case "101":
                return 5;
            case "110":
                return 6;
            case "111":
                return 7;
            default:
                return -1;
        }

     }


    }



