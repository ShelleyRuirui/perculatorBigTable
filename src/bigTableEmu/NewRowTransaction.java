package bigTableEmu;

import java.util.HashMap;
import java.util.Map;

public class NewRowTransaction {
	Row row;
	BigTable fatherTable;
	Map<Column,ValueWithTimestamp> localData;
	Map<Column,Long> prevData;
	long startTimestamp=-1;
	
	public NewRowTransaction(Row row){
		this.row=row;
		localData= new HashMap<Column,ValueWithTimestamp>();
		prevData=new HashMap<Column,Long>();
	}
	
	public void setTable(BigTable table){
		fatherTable=table;
	}	
	
	public String read(Column col,long start_ts,long end_ts){
		return read(col,start_ts,end_ts,true);
	}
	
	public String read(Column col,long start_ts){
		return read(col,start_ts,(long)1,false);
	}
	
	public void write(Column col,long timestamp,String value){
		checkAndSetStartTimestamp(col);
		ValueWithTimestamp tempData=localData.get(col);
		if(tempData==null){   //Note that only one version is stored for each column
			tempData=new ValueWithTimestamp(value,timestamp);
			localData.put(col, tempData);
		}
	}
	
	public void erase(Column col,long timestamp){
		write(col,timestamp,null);
	}
	
	public boolean commit(){
		synchronized (this){
			for(Map.Entry<Column, ValueWithTimestamp> entry:localData.entrySet()){
				Column col=entry.getKey();
				ValueWithTimestamp data=entry.getValue();
				long oldNow=fatherTable.getLatestTimestamp(row, col);
				long oldPrev=prevData.get(col);
//				System.out.println("ROW:"+row);
//				System.out.println("STARTTIMESTAMP:"+startTimestamp);
//				System.out.println("OLD:"+old);
				
				if(oldNow>oldPrev){
					return false; //Some else with a later timestamp has committed first
				}
			}
			//valid to commit
			for(Map.Entry<Column, ValueWithTimestamp> entry:localData.entrySet()){
				Column col=entry.getKey();
				ValueWithTimestamp data=entry.getValue();
				fatherTable.write(row, col, data.timestamp, data.value);
			}
			return true;
		}
	}
	
	private String read(Column col,long start_ts,long end_ts,boolean hasEnd){
		ValueWithTimestamp readVal=null;
		ValueWithTimestamp localVal=null;
		if(hasEnd){
			readVal=fatherTable.read(row, col, start_ts, end_ts);
		}else{
			readVal=fatherTable.read(row, col, start_ts);
		}
		localVal=getLocalValue(col, start_ts, end_ts,hasEnd);
		
		//Needs to merge with local values
		if(readVal==null){      //The data doesn't exist in the table, but may exist in the local
			if(localVal==null)
				return null;
			return localVal.value;
		}
			
		if(localVal==null)   //The data doesn't exist in the local but exists in the table
			return readVal.value;
		
		if(readVal.timestamp<=localVal.timestamp){
			return localVal.value;
		}

		System.out.println("READ: TABLE NEWER THAN LOCAL:"+readVal+" and "+localVal);
		return readVal.value;  //It happens when others modify the table and made seen by others. Not likely
	}
	
	private void checkAndSetStartTimestamp(Column col){
		if(startTimestamp==-1){
			startTimestamp=OracleTimestampEmu.getCurTimestamp();
		}
		Long old=fatherTable.getLatestTimestamp(row, col);
		prevData.put(col, old);
	}
	
	private ValueWithTimestamp getLocalValue(Column col,long start_ts,long end_ts,boolean hasEnd){
		ValueWithTimestamp data=localData.get(col);
		if(data==null)
			return null;
		if(hasEnd){
			if(data.timestamp>=start_ts && data.timestamp<=end_ts)
				return data;
			return null;
		}else{
			if(data.timestamp>=start_ts)
				return data;
			return null;
		}
		
	}
}
