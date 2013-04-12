package bigTableEmu;

import java.util.HashMap;
import java.util.Map;

public class RowData {

	Map<Column,ColumnData> cols=new HashMap<Column,ColumnData>();
	
	public ValueWithTimestamp readCol(Column col,long start_ts,long end_ts){
		ColumnData colsData=cols.get(col);
		return colsData.read(start_ts, end_ts);
	}
}
