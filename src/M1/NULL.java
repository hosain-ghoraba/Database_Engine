package M1;

import java.util.Vector;

// This is a class that represents a null value row in the database (a wrapper class)

public class NULL extends Row implements Comparable<Row>{

	public NULL() {
		super(null);
	}


	public NULL(Vector<Object> d) {
		super(d);
		//TODO Auto-generated constructor stub
	}

	@Override
	public String toString() {
		return "NULL";
	}

	@Override
	public int compareTo(Row o) {
		if(o == null || o instanceof NULL) return 0;
		return -1;
	}
}