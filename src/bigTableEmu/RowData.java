package bigTableEmu;

import java.util.HashMap;
import java.util.Map;

public class RowData {

	Map<Column,ColumnData> cols=new HashMap<Column,ColumnData>();
	
	public ValueWithTimestamp readCol(Column col,long start_ts,long end_ts){
		ColumnData colsData=cols.get(col);
		if(colsData==null)
			return null;
		return colsData.read(start_ts, end_ts);
	}
	
	public ValueWithTimestamp readCol(Column col,long start_ts){
		ColumnData colsData=cols.get(col);
		if(colsData==null)
			return null;
		return colsData.read(start_ts);
	}
	
	public void writeCol(Column col,long timestamp,String value){
		ColumnData colsData=cols.get(col);
		if(colsData==null){
			colsData=new ColumnData();
			cols.put(col, colsData);
		}
		colsData.write(timestamp, value);
	}
	
	public void addColumnAndData(Column col,ColumnData data){
		cols.put(col, data);
	}
	
	public long getLatestTimestamp(Column col){
		ColumnData colsData=cols.get(col);
		if(colsData==null)
			return -1;
		return colsData.getlatestTimestamp();
	}
}
