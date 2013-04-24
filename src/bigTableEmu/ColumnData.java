package bigTableEmu;

import java.util.Set;
import java.util.TreeMap;

public class ColumnData {

	TreeMap<Long, String> versions = new TreeMap<Long, String>();

	public ValueWithTimestamp read(long start_ts, long end_ts) {
		return read(start_ts, end_ts, true);
	}

	public ValueWithTimestamp read(long start_ts) {
		return read(start_ts, (long) 1, false);
	}
	

	private ValueWithTimestamp read(long start_ts, long end_ts, boolean hasEnd) {
		Set<Long> set = versions.descendingKeySet();
		long valueTimestamp = -1;
		String value = null;
		// Find latest version in the interval
		for (Long version : set) {
			if (version < start_ts)
				break;
			if (hasEnd) {
				if (version <= end_ts) {
					valueTimestamp = version;
					value = versions.get(version);
					break;
				}
			} else {
				// No end
				valueTimestamp = version;
				value = versions.get(version);
				break;
			}
		}
		if (valueTimestamp == -1)
			return null;
		return new ValueWithTimestamp(value, valueTimestamp);
	}

	public void write(long timestamp, String value) {
		versions.put(timestamp, value);
	}
	
	public long getlatestTimestamp(){
		Set<Long> set = versions.descendingKeySet();
		Long timestamp=set.iterator().next();
		return timestamp;
	}
	
	public void print(){
		Set<Long> set = versions.descendingKeySet();
		for(Long version:set){
			String value=versions.get(version);
			System.out.println("\t\t"+version+"->"+value);
		}
	}
	
	public static void main(String[] args) {
		ColumnData data=new ColumnData();
		data.write(7, "Hi7");
		data.write(10, "Hi10");
		data.write(3, "Hi3");
		data.print();
		System.out.println(data.read(2));
		System.out.println(data.read(5));
		System.out.println(data.read(6));
		System.out.println(data.read(5));
		System.out.println(data.read(9));
		System.out.println(data.read(12));
		
		System.out.println(data.read(1,2));
		System.out.println(data.read(2,3));
		System.out.println(data.read(2,4));	
		System.out.println(data.read(4,6));
		System.out.println(data.read(7,9));
		System.out.println(data.read(6,9));
		System.out.println(data.read(7,10));
		System.out.println(data.read(10,12));
		System.out.println(data.read(11,15));

	}
}
