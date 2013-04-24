package perculator;

import bigTableEmu.BigTable;
import bigTableEmu.Column;
import bigTableEmu.Row;

public class Write {

	Row row;
	Column col;
	String value;
	String table;
	
	public Write(String table,Row row, Column col, String value) {
		this.table=table;
		this.row = row;
		this.col = col;
		this.value = value;
	}
	
	public String toString(){
		return "Table:"+table+"Row:"+row+"Column:"+col+"Value:"+value;
	}
	
}
