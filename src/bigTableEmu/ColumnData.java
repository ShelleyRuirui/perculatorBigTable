package bigTableEmu;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ColumnData {

	Map<Long,String> versions=new HashMap<Long,String>();
	
	public ValueWithTimestamp read(long start_ts,long end_ts){
		Set<Long> set=versions.keySet();
		long largest=-1;
		for(Long version:set){
			if(version>=start_ts&&version<=end_ts&&version>largest){
				largest=version;
			}
		}
		if(largest==-1)
			return null;
		String value=versions.get(largest);
		ValueWithTimestamp result=new ValueWithTimestamp(value,largest);
		return result;
	}
	
	public void addValue(long timestamp,String value){
		versions.put(timestamp, value);
	}
}
