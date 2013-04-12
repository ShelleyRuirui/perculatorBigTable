package bigTableEmu;

import java.util.HashMap;
import java.util.Map;

public class BigTable {
	
	Map<Row,RowData> rows=new HashMap<Row,RowData>();

	public RowTransaction startRowTransaction(Row row){
		//TODO 
		RowTransaction tr=new RowTransaction(row);
		tr.setTable(this);
		return tr;
	}
	
	public ValueWithTimestamp read(Row row,Column col,long start_ts,long end_ts){
		System.out.println("Output map");
//		for(Map.Entry<Row, RowData> entry:rows.entrySet()){
//			System.out.println(entry.getKey().getRowKey());
//		}
		RowData rowData=rows.get(row);
		if(rowData==null)
			return null;
		return rowData.readCol(col, start_ts, end_ts);
	}
	
	public void addRowAndData(Row row,RowData data){
		rows.put(row, data);
	}
}
