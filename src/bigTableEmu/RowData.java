package bigTableEmu;

import java.util.HashMap;
import java.util.Map;

public class RowData {

	Map<Column,ColumnData> cols=new HashMap<Column,ColumnData>();
	
	public ValueWithTimestamp readCol(Column col,long start_ts,long end_ts){
		ColumnData colsData=cols.get(col);
//		for(Map.Entry<Column, ColumnData> entry:cols.entrySet()){
//			System.out.println(entry.getKey().getColName());
//		}
		if(colsData==null)
			return null;
		return colsData.read(start_ts, end_ts);
	}
	
	public void addColumnAndData(Column col,ColumnData data){
		cols.put(col, data);
	}
}
