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
					return entry.getValue();
				}
				else
					continue;
			}else{
				Long secondTime=Long.parseLong(timespan[1]);
				if(end_ts>=firstTime&&end_ts<=secondTime){
					containsTimestamp=true;
					return entry.getValue();
				}else
					continue;
			}
		}
		return null;
	}
	
	public boolean hasTimestampValue(){
		return containsTimestamp;
	}
}
