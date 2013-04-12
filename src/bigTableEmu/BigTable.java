package bigTableEmu;

import java.util.HashMap;
import java.util.Map;

public class BigTable {
	
	Map<Row,RowData> rows=new HashMap<Row,RowData>();

	public RowTransaction startRowTransaction(String tableName,Row row){
		//TODO 
		RowTransaction tr=new RowTransaction(row);
		tr.setTable(this);
		return tr;
	}
	
	public ValueWithTimestamp read(Row row,Column col,long start_ts,long end_ts){
		RowData rowData=rows.get(row);
		return rowData.readCol(col, start_ts, end_ts);
	}
}
