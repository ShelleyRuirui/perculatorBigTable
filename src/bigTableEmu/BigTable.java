package bigTableEmu;

import java.util.HashMap;
import java.util.Map;

public class BigTable {
	
	Map<Row,RowData> rows=new HashMap<Row,RowData>();

	public NewRowTransaction startRowTransaction(Row row){
		//TODO 
		NewRowTransaction tr=new NewRowTransaction(row);
		tr.setTable(this);
		return tr;
	}
	
	public ValueWithTimestamp read(Row row,Column col,long start_ts,long end_ts){
		RowData rowData=rows.get(row);
		if(rowData==null)
			return null;
		return rowData.readCol(col, start_ts, end_ts);
	}
	
	public ValueWithTimestamp read(Row row,Column col,long start_ts){
		RowData rowData=rows.get(row);
		if(rowData==null)
			return null;
		return rowData.readCol(col, start_ts);
	}
	
	public void addRowAndData(Row row,RowData data){
		rows.put(row, data);
	}
	
	public void write(Row row,Column col,long timestamp,String value){
		RowData rowData=rows.get(row);
		if(rowData==null){
			rowData=new RowData();
			rows.put(row, rowData);
		}
		rowData.writeCol(col, timestamp, value);	
	}
	
	public long getLatestTimestamp(Row row,Column col){
		RowData rowData=rows.get(row);
		if(rowData==null)
			return -1;
		return rowData.getLatestTimestamp(col);		
	}
	
	public void print(){
		System.out.println("****************");
		for(Map.Entry<Row, RowData> entry:rows.entrySet()){
			System.out.println(entry.getKey());
			entry.getValue().print();
			System.out.println("****************");
		}
	}
}
