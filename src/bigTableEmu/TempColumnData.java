package bigTableEmu;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class TempColumnData {

	Map<String,String> optimizedData=new HashMap<String,String>();
	boolean containsTimestamp=false;
	
	public String getRightResult(long end_ts){
		containsTimestamp=false;
		for (Map.Entry<String, String> entry : optimizedData.entrySet()){
			String times=entry.getValue();
			String[] timespan=times.split(",");
			Long firstTime=Long.parseLong(timespan[0]);
			if(timespan.length==1){
				if(end_ts==firstTime){
					containsTimestamp=true;
					return entry.getKey();
				}
				else
					continue;
			}else{
				Long secondTime=Long.parseLong(timespan[1]);
				if(end_ts>=firstTime&&end_ts<=secondTime){
					containsTimestamp=true;
					return entry.getKey();
				}else
					continue;
			}
		}
		return null;
	}
	
	public boolean hasTimestampValue(){
		return containsTimestamp;
	}
	
	public void addCachedValue(String value,long value_ts,long orig_end_ts){
		if(orig_end_ts<value_ts){
			System.out.println("Error in addCachedValue");
			return;
		}
		String timespan=value_ts+","+orig_end_ts;
		optimizedData.put(value, timespan);
	}
}
