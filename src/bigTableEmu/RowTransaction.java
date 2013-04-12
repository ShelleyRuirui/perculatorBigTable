package bigTableEmu;

import java.util.HashMap;
import java.util.Map;

public class RowTransaction {
	Row currentRow;
	Map<Column,String> tempWriteValues;
	Map<Column,TempColumnData> tempReadValues;
	long startTimestamp;
	BigTable fatherTable;
	
	public void setTable(BigTable b){
		fatherTable=b;
	}

	public RowTransaction(Row row){
		currentRow=row;
		tempWriteValues=new HashMap<Column,String>();
		tempReadValues=new HashMap<Column,TempColumnData>();
		startTimestamp=-1;
	}
	
	public String read(Row row,Column col,long start_ts,long end_ts){
		//TODO
		checkAndSetTimestamps();
		
		//first get relevant version from local stored values
		TempColumnData curItems=tempReadValues.get(col);
		
		String localResult=curItems.getRightResult(end_ts);
		if(localResult!=null)
			return localResult;
		
		if(curItems.containsTimestamp)
			return null;
		
		//If not stored locally,first fetch it from the original table and then store it in the local
		ValueWithTimestamp readVal=fatherTable.read(row, col, start_ts, end_ts);
		//Add to local
		if(readVal==null)
			return null;
		
		String result="";
		return result;
	}
	
	public String read(Row row,Column col,long start_ts){
		String result="";
		return result;
	}
	
	public void write(Row row,Column col,long commit_ts,long start_ts){
		//TODO
		//Add to local write value
		
		//Able to read by own read
	}
	
	public void erase(Row row,Column col,long commit_ts){
		//TODO
	}
	
	public boolean commit(){
		//TODO
		boolean result=false;
		return result;
	}
	
	private void checkAndSetTimestamps(){
		if(startTimestamp==-1){
			startTimestamp=OracleTimestampEmu.getCurTimestamp();
		}
	}
	
	private String getLocalValue(String colName){
		return tempWriteValues.get(colName);
	}
}
