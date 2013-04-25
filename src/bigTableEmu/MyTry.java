package bigTableEmu;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public class MyTry {

	public static void main(String[] args) {
		 TreeMap<Long, String> treemap = null;
	      
	      // populating tree map
	      treemap.put((long) 2, "two");
	      treemap.put((long) 1, "one");
	      treemap.put((long) 3, "three");
	      treemap.put((long) 6, "six");
	      treemap.put((long) 5, "five");
	      
	      // putting values in navigable set
	      Set<Long> set=treemap.descendingKeySet();
	      
	      System.out.println("Checking value");
	      System.out.println(set);
//	      for(Entry<Long,String> entry:map.entrySet()){
//	    	  System.out.println(entry.getKey());
//	    	  System.out.println(entry.getValue());
//	    	  System.out.println();
//	      }
	}
}
