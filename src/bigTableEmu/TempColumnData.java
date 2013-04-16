package bigTableEmu;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class TempColumnData {

	//[value,ts_ts]
	Map<String, String> optimizedData = new HashMap<String, String>();
	boolean containsTimestamp = false;

	public String getRightResult(long start_ts,long end_ts) {
		String tempResult=null;
		for (Map.Entry<String, String> entry : optimizedData.entrySet()) {
			String times = entry.getValue();
			String[] timespan = times.split(",");
			Long firstTime = Long.parseLong(timespan[0]);
			Long secondTime = Long.parseLong(timespan[1]);
			if (end_ts >= firstTime && end_ts <= secondTime) {
				return entry.getKey();
			} else if(!(start_ts>secondTime || end_ts<firstTime)){
				tempResult=entry.getKey();
			}				
		}
		return tempResult;
	}

	public void addCachedValue(String value, long value_ts, long orig_end_ts) {
		if (orig_end_ts < value_ts) {
			System.out.println("Error in addCachedValue");
			return;
		}
		String timespan = value_ts + "," + orig_end_ts;
		optimizedData.put(value, timespan);
	}

	public void splitAndInsert(String value, long split_ts) {
		for (Map.Entry<String, String> entry : optimizedData.entrySet()) {
			String times = entry.getValue();
			String[] timespan = times.split(",");
			Long firstTime = Long.parseLong(timespan[0]);
			if (split_ts < firstTime)
				continue;
			Long secondTime = Long.parseLong(timespan[1]);
			if (split_ts == firstTime) {
				if(split_ts==secondTime){
					entry.setValue(value);
					return;
				}else{
					String val1=firstTime+","+firstTime;
					String val2=(split_ts+1)+","+secondTime;
					optimizedData.put(value,val1);
					String prevKey=entry.getKey();
					optimizedData.put(prevKey,val2);
					return;
				}
			}
			if(split_ts>firstTime && split_ts<=secondTime){
				String val1=firstTime+","+(split_ts-1);
				String val2=split_ts+","+secondTime;
				String prevKey=entry.getKey();
				optimizedData.put(prevKey,val1);
				optimizedData.put(value,val2);
				return;
			}
			if(split_ts<firstTime || split_ts>secondTime){
				continue;
			}
		}
		optimizedData.put(value, split_ts+","+split_ts);
	}
}
